package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

// Configuration constants
const (
	SparklineChars     = " ▂▃▄▅▆▇█"
	SparklineLength    = 60  // fallback trend width before the terminal size is known
	MaxHistory         = 800 // in-flight samples retained for the trend
	DepthWarnThreshold = 100
	DepthCritThreshold = 1000
	DefaultInterval    = 2 // seconds

	// Bounds for the runtime-adjustable refresh interval (+/- keys).
	MinInterval  = 200 * time.Millisecond
	MaxInterval  = 10 * time.Second
	IntervalStep = 200 * time.Millisecond

	// Smoothing factor for the incoming-rate moving average (0..1); higher
	// reacts faster, lower is smoother.
	RateEMAAlpha = 0.4
)

// Data structures
type Producer struct {
	BroadcastAddress string `json:"broadcast_address"`
	HTTPPort         int    `json:"http_port"`
}

type NodesResponse struct {
	Producers []Producer `json:"producers"`
}

type Channel struct {
	ChannelName   string        `json:"channel_name"`
	Depth         int           `json:"depth"`
	BackendDepth  int           `json:"backend_depth"`
	InFlightCount int           `json:"in_flight_count"`
	MessageCount  int64         `json:"message_count"`
	TimeoutCount  int64         `json:"timeout_count"`
	RequeueCount  int64         `json:"requeue_count"`
	Clients       []ClientStats `json:"clients"`
}

// ClientStats is the per-client stats nsqd returns for each channel. We decode
// the fields the detail view shows; everything else (tls, version, etc.) is
// dropped on the floor.
type ClientStats struct {
	ClientID      string `json:"client_id"`
	Hostname      string `json:"hostname"`
	RemoteAddress string `json:"remote_address"`
	State         int    `json:"state"`
	ReadyCount    int64  `json:"ready_count"`
	InFlightCount int64  `json:"in_flight_count"`
	MessageCount  int64  `json:"message_count"`
	FinishCount   int64  `json:"finish_count"`
	RequeueCount  int64  `json:"requeue_count"`
	ConnectTs     int64  `json:"connect_ts"`
}

type Topic struct {
	TopicName    string    `json:"topic_name"`
	MessageCount int64     `json:"message_count"`
	Channels     []Channel `json:"channels"`
}

type StatsResponse struct {
	Topics []Topic        `json:"topics"`
	Data   *StatsResponse `json:"data,omitempty"` // For newer NSQ versions
}

// Totals holds cluster-wide figures shown in the summary panel.
type Totals struct {
	Inflight       int
	MessageCount   int64   // total messages produced across all topics
	Processed      int64   // total messages processed across all channels
	IncomingPerSec float64 // smoothed global incoming rate
}

type ChannelData struct {
	Topic             string
	Channel           string
	Depth             int
	InFlightCount     int
	ClientCount       int     // active consumer connections (sum across nsqd nodes)
	ReadyCount        int64   // sum of RDY across the channel's clients
	MessageCount      int64   // cumulative messages processed by the channel
	IncomingPerSecond float64 // rate of messages produced to the topic
	IncomingPerMinute float64
	TimeoutCount      int64
	RequeueCount      int64
	TimeoutRate       float64 // smoothed growth per second
	RequeueRate       float64
}

// TopicData is a per-topic rollup of its channels, used by the topic view to
// surface which topics are backing up.
type TopicData struct {
	Topic             string
	ChannelCount      int
	ConnectionCount   int // sum of client connections across the topic's channels
	ReadyCount        int64
	Depth             int // sum of channel depths
	InFlightCount     int
	IncomingPerSecond float64
	IncomingPerMinute float64
	MessageCount      int64 // produced-message counter for the topic
}

type NSQTop struct {
	app                *tview.Application
	table              *tview.Table
	summary            *tview.TextView
	trend              *tview.TextView
	filterInput        *tview.InputField
	flex               *tview.Flex
	client             *http.Client
	clusterName        string // optional label shown in the status panel
	lookupURLs         []string
	nsqdURLs           []string
	intervalNanos      atomic.Int64       // current refresh interval; adjustable at runtime
	intervalCh         chan time.Duration // signals the monitor goroutine to retune its ticker
	previousTopicStats map[string]int64
	topicRateEMA       map[string]float64 // smoothed incoming rate (msgs/sec) per topic
	prevTimeoutCount   map[string]int64   // previous timeout_count per topic/channel
	prevRequeueCount   map[string]int64   // previous requeue_count per topic/channel
	timeoutRateEMA     map[string]float64 // smoothed timeout growth per topic/channel
	requeueRateEMA     map[string]float64 // smoothed requeue growth per topic/channel
	trendHistory       []int              // per-sample traffic (processed this interval + in-flight)
	prevProcessed      int64              // previous total processed, for the per-interval delta
	havePrevProcessed  bool
	scrollTop          bool

	// Topic/channel substring filter, toggled with "/".
	filtering  bool
	filterText string

	// Count baseline, toggled with "c"/"C". When active, the cumulative columns
	// (Processed, Timeouts, Requeues) and Total Msgs are shown as the delta since
	// the baseline was captured, so changes are easy to read at a glance.
	baselineActive bool
	baselineAt     time.Time
	baseProcessed  map[string]int64 // per topic/channel MessageCount at capture
	baseTimeout    map[string]int64
	baseRequeue    map[string]int64
	baseTotalMsgs  int64

	// Sorting state, driven by the arrow keys. sortColumn/sortDesc track the
	// active view; savedSorts preserves the inactive views' selections so
	// switching between them restores each one's last sort.
	sortColumn int
	sortDesc   bool
	savedSorts [viewCount]struct {
		col  int
		desc bool
	}

	// viewMode picks the table being rendered: viewChannels, viewTopics,
	// viewChannelDetail, or viewTopicDetail.
	viewMode int

	// Topic baseline: the "c" baseline also captures per-topic message counts so
	// the topic view's Messages column can show the delta since the baseline.
	baseTopicMsgs map[string]int64

	// Last rendered snapshot, so a key press can re-sort without waiting for
	// the next refresh tick. lastRaw keeps the per-node responses so detail
	// views can derive their rows without another HTTP round-trip.
	lastChannels []*ChannelData
	lastTopics   []*TopicData
	lastTotals   Totals
	lastNodes    []string
	lastRaw      []nodeStats

	// drillTarget identifies which topic (in viewTopicDetail) or topic/channel
	// (in viewChannelDetail) the detail view is drilled into.
	drillTarget string
}

// columnTitles is the channel view's column order; sortColumn indexes into it.
var columnTitles = []string{"Topic/Channel", "Depth", "In-Flight", "Ready", "In/sec", "In/min", "Processed", "Timeouts", "Requeues"}

// topicColumnTitles is the topic view's column order; it has its own sort state.
var topicColumnTitles = []string{"Topic", "Channels", "Conns", "Depth", "In-Flight", "Ready", "In/sec", "In/min", "Messages"}

const (
	sortColumnDepth      = 1 // Depth in the channel view
	sortColumnTopicDepth = 3 // Depth in the topic view
)

const (
	viewChannels      = 0
	viewTopics        = 1
	viewChannelDetail = 2
	viewTopicDetail   = 3
	viewCount         = 4
)

// Sort column defaults for the detail views (indexes into their column lists).
const (
	sortColumnTopicDetailDepth   = 2 // Depth in the topic detail view
	sortColumnChannelDetailFlight = 5 // In-Flight in the channel detail view
)

// channelDetailColumns and topicDetailColumns are the column orders for the
// per-client and per-node drill-down views.
var channelDetailColumns = []string{"Client", "Hostname", "Remote", "State", "Ready", "In-Flight", "Processed", "Requeued", "Connected"}
var topicDetailColumns = []string{"NSQd Server", "Channels", "Depth", "In-Flight", "Conns", "Ready", "Messages"}

// CLI configuration
var (
	lookupAddresses string
	nsqdAddresses   string
	refreshInterval int
	clusterName     string
)

func main() {
	// Default to 24-bit color so the dark palette renders accurately. Without
	// this, tcell downsamples our hex colors to the terminal's ANSI palette,
	// which themes them unpredictably — notably inside `kubectl run -it` pods
	// where COLORTERM is unset. Honored only when the user hasn't set it; they
	// can still opt out with TCELL_TRUECOLOR=disable.
	if os.Getenv("COLORTERM") == "" {
		os.Setenv("COLORTERM", "truecolor")
	}

	var rootCmd = &cobra.Command{
		Use:   "nsqtop",
		Short: "A top-like monitoring tool for NSQ clusters",
		Long:  "Monitor NSQ clusters in real-time with a terminal-based interface",
		Run:   runNSQTop,
	}

	// Get defaults from environment variables
	defaultLookupd := getEnvWithFallback("NSQTOP_LOOKUPD_ADDRESSES",
		getEnvWithFallback("NSQTOP_LOOKUPD_ADDRESS", ""))
	defaultNSQD := getEnvWithFallback("NSQTOP_NSQD_ADDRESSES",
		getEnvWithFallback("NSQTOP_NSQD_ADDRESS", ""))
	defaultInterval := getEnvIntWithFallback("NSQTOP_INTERVAL", DefaultInterval)
	defaultName := getEnvWithFallback("NSQTOP_CLUSTER_NAME", "")

	rootCmd.Flags().StringVarP(&lookupAddresses, "lookupd-http-address", "l", defaultLookupd,
		"Comma-separated HTTP addresses of nsqlookupd instances (e.g., localhost:4161)")
	rootCmd.Flags().StringVarP(&nsqdAddresses, "nsqd-http-address", "n", defaultNSQD,
		"Comma-separated HTTP addresses of nsqd instances; queried directly, bypassing nsqlookupd (e.g., localhost:4151)")
	rootCmd.Flags().IntVarP(&refreshInterval, "interval", "i", defaultInterval,
		"Refresh interval in seconds")
	rootCmd.Flags().StringVar(&clusterName, "name", defaultName,
		"Label for this cluster, shown in the status panel (helps tell instances apart)")

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func runNSQTop(cmd *cobra.Command, args []string) {
	lookupURLs := normalizeAddresses(lookupAddresses)
	nsqdURLs := normalizeAddresses(nsqdAddresses)

	if len(lookupURLs) == 0 && len(nsqdURLs) == 0 {
		log.Fatal("provide --lookupd-http-address or --nsqd-http-address")
	}

	interval := time.Duration(refreshInterval) * time.Second
	if interval < MinInterval {
		interval = MinInterval
	}
	if interval > MaxInterval {
		interval = MaxInterval
	}

	nsqTop := &NSQTop{
		clusterName:        strings.TrimSpace(clusterName),
		lookupURLs:         lookupURLs,
		nsqdURLs:           nsqdURLs,
		intervalCh:         make(chan time.Duration, 1),
		previousTopicStats: make(map[string]int64),
		topicRateEMA:       make(map[string]float64),
		prevTimeoutCount:   make(map[string]int64),
		prevRequeueCount:   make(map[string]int64),
		timeoutRateEMA:     make(map[string]float64),
		requeueRateEMA:     make(map[string]float64),
		trendHistory:       make([]int, 0, MaxHistory),
		scrollTop:          true,
		sortColumn:         sortColumnDepth,
		sortDesc:           true,
		viewMode:           viewChannels,
		client:             &http.Client{Timeout: 5 * time.Second},
	}
	// Per-view sort defaults; the active view (channels) reads its sort from
	// sortColumn/sortDesc above. The rest live in savedSorts until their view
	// becomes active.
	nsqTop.savedSorts[viewChannels] = struct {
		col  int
		desc bool
	}{sortColumnDepth, true}
	nsqTop.savedSorts[viewTopics] = struct {
		col  int
		desc bool
	}{sortColumnTopicDepth, true}
	nsqTop.savedSorts[viewTopicDetail] = struct {
		col  int
		desc bool
	}{sortColumnTopicDetailDepth, true}
	nsqTop.savedSorts[viewChannelDetail] = struct {
		col  int
		desc bool
	}{sortColumnChannelDetailFlight, true}
	nsqTop.intervalNanos.Store(int64(interval))

	nsqTop.initUI()
	nsqTop.startMonitoring()

	if err := nsqTop.app.Run(); err != nil {
		log.Fatal(err)
	}
}

// Dark color palette (Tokyo Night inspired), used for the theme and table cells.
var (
	colorBg       = tcell.NewHexColor(0x1a1b26)
	colorFg       = tcell.NewHexColor(0xc0caf5)
	colorDim      = tcell.NewHexColor(0x565f89)
	colorBorder   = tcell.NewHexColor(0x3b4261)
	colorAccent   = tcell.NewHexColor(0x7aa2f7)
	colorHeaderBg = tcell.NewHexColor(0x283457)
	colorOK       = tcell.NewHexColor(0x9ece6a)
	colorWarn     = tcell.NewHexColor(0xe0af68)
	colorCrit     = tcell.NewHexColor(0xf7768e)
	// colorColumnTint is a barely-there stripe behind the active sort column,
	// just enough to lift it above the bg without competing with row selection.
	colorColumnTint = tcell.NewHexColor(0x1f2235)
)

// applyDarkTheme switches tview's global theme to a dark palette. It must run
// before any widgets are created so they pick up the new defaults.
func applyDarkTheme() {
	tview.Styles = tview.Theme{
		PrimitiveBackgroundColor:    colorBg,
		ContrastBackgroundColor:     colorHeaderBg,
		MoreContrastBackgroundColor: colorBorder,
		BorderColor:                 colorBorder,
		TitleColor:                  colorAccent,
		GraphicsColor:               colorBorder,
		PrimaryTextColor:            colorFg,
		SecondaryTextColor:          colorDim,
		TertiaryTextColor:           colorAccent,
		InverseTextColor:            colorBg,
		ContrastSecondaryTextColor:  colorFg,
	}
}

func (n *NSQTop) initUI() {
	applyDarkTheme()
	n.app = tview.NewApplication()

	// Create summary view
	n.summary = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(false)
	summaryTitle := "NSQ Cluster Status"
	if n.clusterName != "" {
		summaryTitle = "NSQ Cluster Status — " + n.clusterName
	}
	n.summary.SetBorder(true).SetTitle(summaryTitle)

	// Full-width in-flight trend strip, shown right above the table.
	n.trend = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(false)
	n.trend.SetBorder(true).SetTitle("Traffic Trend (processed + in-flight)")

	// Create table. No cell-border grid: rows stay single-spaced (denser), and
	// the header is distinguished by an underline instead (see updateUI). It
	// sits in its own bordered box with inner padding so the text isn't flush
	// against the edge, matching the panels above.
	// Rows are selectable so Enter can drill into the highlighted row;
	// columns are not, because sorting is driven by ←/→ instead.
	n.table = tview.NewTable().
		SetBorders(false).
		SetSelectable(true, false)
	n.table.SetBorder(true).SetTitle("Channels").SetBorderPadding(0, 0, 1, 1)

	// Filter bar, hidden (height 0) until "/" is pressed. Live-filters as you
	// type; Enter keeps the filter, Esc clears it.
	n.filterInput = tview.NewInputField().SetLabel(" / ")
	n.filterInput.SetChangedFunc(func(text string) {
		n.filterText = strings.TrimSpace(text)
		n.scrollTop = true
		n.redraw()
	})
	n.filterInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEscape {
			n.filterText = ""
			n.filterInput.SetText("")
		}
		n.stopFilter()
	})

	// Layout: status panel, full-width trend strip, table, filter bar.
	n.flex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(n.summary, 7, 1, false).
		AddItem(n.trend, 3, 1, false).
		AddItem(n.table, 0, 1, true).
		AddItem(n.filterInput, 0, 0, false)

	n.app.SetRoot(n.flex, true).SetFocus(n.flex)

	// Key bindings: Ctrl+C quits, Left/Right pick the sort column, Enter
	// reverses the sort direction, -/+ change the refresh rate, "/" filters.
	// Up/Down (and friends) fall through to the table so it can still scroll.
	n.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// While typing a filter, let keys reach the input field (its DoneFunc
		// handles Enter/Esc); only Ctrl+C still quits.
		if n.filtering {
			if event.Key() == tcell.KeyCtrlC {
				n.app.Stop()
				return nil
			}
			return event
		}

		switch event.Key() {
		case tcell.KeyCtrlC:
			n.app.Stop()
			return nil
		case tcell.KeyEscape:
			// First Esc exits a detail view back to its parent; otherwise
			// clear any active filter from the main view.
			if n.drillOut() {
				return nil
			}
			if n.filterText != "" {
				n.filterText = ""
				n.scrollTop = true
				n.redraw()
			}
			return nil
		case tcell.KeyLeft:
			n.changeSortColumn(-1)
			return nil
		case tcell.KeyRight:
			n.changeSortColumn(1)
			return nil
		case tcell.KeyEnter:
			n.drillIn()
			return nil
		case tcell.KeyTab, tcell.KeyBacktab:
			n.toggleView()
			return nil
		case tcell.KeyRune:
			switch event.Rune() {
			case ' ': // reverse the active sort direction
				n.sortDesc = !n.sortDesc
				n.scrollTop = true
				n.redraw()
				return nil
			case '/': // open the topic/channel filter
				n.startFilter()
				return nil
			case '-', '_': // smaller interval -> faster refresh
				n.adjustInterval(-IntervalStep)
				return nil
			case '+', '=': // larger interval -> slower refresh
				n.adjustInterval(IntervalStep)
				return nil
			case 'c': // zero the cumulative counters (re-zero on each press)
				n.zeroCounts()
				return nil
			case 'C': // clear the baseline, back to absolute totals
				n.clearBaseline()
				return nil
			}
		}
		return event
	})
}

// startFilter reveals the filter bar and focuses it for input.
func (n *NSQTop) startFilter() {
	n.filtering = true
	n.filterInput.SetText(n.filterText)
	n.flex.ResizeItem(n.filterInput, 1, 0)
	n.app.SetFocus(n.filterInput)
}

// stopFilter hides the filter bar and returns focus to the table.
func (n *NSQTop) stopFilter() {
	n.filtering = false
	n.flex.ResizeItem(n.filterInput, 0, 0)
	n.app.SetFocus(n.table)
	n.scrollTop = true
	n.redraw()
}

// drillIn handles Enter on the selected row. From the topic view, it opens a
// per-nsqd-node breakdown of that topic; from the channel view, it opens a
// per-client breakdown of that channel. Esc backs out.
func (n *NSQTop) drillIn() {
	if n.viewMode != viewTopics && n.viewMode != viewChannels {
		return
	}
	row, _ := n.table.GetSelection()
	if row < 1 {
		return
	}
	cell := n.table.GetCell(row, 0)
	if cell == nil {
		return
	}
	target, ok := cell.GetReference().(string)
	if !ok || target == "" {
		return
	}
	n.drillTarget = target
	if n.viewMode == viewTopics {
		n.switchView(viewTopicDetail)
	} else {
		n.switchView(viewChannelDetail)
	}
	n.redraw()
}

// drillOut returns from a detail view to its parent main view, clearing the
// drill target. Returns true if a detail view was actually closed; false lets
// Esc fall through to its normal "clear filter" behavior.
func (n *NSQTop) drillOut() bool {
	switch n.viewMode {
	case viewTopicDetail:
		n.drillTarget = ""
		n.switchView(viewTopics)
		n.redraw()
		return true
	case viewChannelDetail:
		n.drillTarget = ""
		n.switchView(viewChannels)
		n.redraw()
		return true
	}
	return false
}

// changeSortColumn moves the active sort column by delta (wrapping around) and
// resets to a sensible default direction: ascending for the name column,
// descending for the numeric ones.
func (n *NSQTop) changeSortColumn(delta int) {
	cols := len(n.activeColumns())
	n.sortColumn = (n.sortColumn + delta + cols) % cols
	n.sortDesc = n.sortColumn != 0
	n.scrollTop = true
	n.redraw()
}

// activeColumns returns the column titles for the active view.
func (n *NSQTop) activeColumns() []string {
	switch n.viewMode {
	case viewTopics:
		return topicColumnTitles
	case viewTopicDetail:
		return topicDetailColumns
	case viewChannelDetail:
		return channelDetailColumns
	default:
		return columnTitles
	}
}

// switchView changes the active view, saving the outgoing view's sort and
// restoring the incoming view's, so per-view sort selections are preserved.
func (n *NSQTop) switchView(next int) {
	if next == n.viewMode {
		return
	}
	n.savedSorts[n.viewMode] = struct {
		col  int
		desc bool
	}{n.sortColumn, n.sortDesc}
	n.viewMode = next
	n.sortColumn = n.savedSorts[next].col
	n.sortDesc = n.savedSorts[next].desc
	n.table.SetTitle(n.tableTitle())
	n.scrollTop = true
}

// toggleView cycles between the main channels and topics tables. From a detail
// view it goes back to the parent main view first; the next Tab toggles between
// the two main views as usual.
func (n *NSQTop) toggleView() {
	switch n.viewMode {
	case viewChannels:
		n.switchView(viewTopics)
	case viewTopics:
		n.switchView(viewChannels)
	case viewChannelDetail:
		n.switchView(viewChannels)
	case viewTopicDetail:
		n.switchView(viewTopics)
	}
	n.redraw()
}

// tableTitle returns the title to show on the table panel for the active view,
// including the drill target for detail views.
func (n *NSQTop) tableTitle() string {
	switch n.viewMode {
	case viewTopics:
		return "Topics"
	case viewTopicDetail:
		return fmt.Sprintf("Topic Detail — %s (Esc to back out)", n.drillTarget)
	case viewChannelDetail:
		return fmt.Sprintf("Channel Detail — %s (Esc to back out)", n.drillTarget)
	default:
		return "Channels"
	}
}

// getInterval returns the current refresh interval (safe across goroutines).
func (n *NSQTop) getInterval() time.Duration {
	return time.Duration(n.intervalNanos.Load())
}

// adjustInterval changes the refresh interval by delta, clamped to
// [MinInterval, MaxInterval], retunes the monitor goroutine's ticker, and
// redraws so the new value is shown immediately. A negative delta speeds up.
func (n *NSQTop) adjustInterval(delta time.Duration) {
	cur := n.getInterval()
	next := cur + delta
	if next < MinInterval {
		next = MinInterval
	}
	if next > MaxInterval {
		next = MaxInterval
	}
	if next == cur {
		return
	}
	n.intervalNanos.Store(int64(next))

	// Non-blocking send; the buffered channel coalesces rapid presses.
	select {
	case n.intervalCh <- next:
	default:
	}
	n.redraw()
}

// redraw re-renders the table from the last snapshot when sorting changes
// between refresh ticks. It is only called from key handlers, which already run
// on tview's main goroutine, so it updates the UI directly — calling
// QueueUpdateDraw from that goroutine would deadlock. tview redraws on its own
// once the input handler returns.
func (n *NSQTop) redraw() {
	if n.lastChannels == nil {
		return
	}
	n.updateUI(n.lastChannels, n.lastTopics, n.lastTotals, n.lastNodes)
}

// zeroCounts captures the current cumulative counters as a baseline so the
// Processed/Timeouts/Requeues columns and Total Msgs show the delta since this
// moment. Pressing "c" again re-zeroes from the latest snapshot.
func (n *NSQTop) zeroCounts() {
	if n.lastChannels == nil {
		return
	}
	n.baseProcessed = make(map[string]int64, len(n.lastChannels))
	n.baseTimeout = make(map[string]int64, len(n.lastChannels))
	n.baseRequeue = make(map[string]int64, len(n.lastChannels))
	for _, c := range n.lastChannels {
		key := c.Topic + "/" + c.Channel
		n.baseProcessed[key] = c.MessageCount
		n.baseTimeout[key] = c.TimeoutCount
		n.baseRequeue[key] = c.RequeueCount
	}
	n.baseTopicMsgs = make(map[string]int64, len(n.lastTopics))
	for _, t := range n.lastTopics {
		n.baseTopicMsgs[t.Topic] = t.MessageCount
	}
	n.baseTotalMsgs = n.lastTotals.MessageCount
	n.baselineActive = true
	n.baselineAt = time.Now()
	n.scrollTop = true
	n.redraw()
}

// clearBaseline discards the baseline, returning the cumulative columns to
// nsqd's absolute lifetime totals.
func (n *NSQTop) clearBaseline() {
	if !n.baselineActive {
		return
	}
	n.baselineActive = false
	n.scrollTop = true
	n.redraw()
}

// dispProcessed/dispTimeout/dispRequeue return the value to show for a channel:
// the raw cumulative count, or the delta since the baseline when one is active.
func (n *NSQTop) dispProcessed(c *ChannelData) int64 {
	if n.baselineActive {
		return sub(c.MessageCount, n.baseProcessed[c.Topic+"/"+c.Channel])
	}
	return c.MessageCount
}

func (n *NSQTop) dispTimeout(c *ChannelData) int64 {
	if n.baselineActive {
		return sub(c.TimeoutCount, n.baseTimeout[c.Topic+"/"+c.Channel])
	}
	return c.TimeoutCount
}

func (n *NSQTop) dispRequeue(c *ChannelData) int64 {
	if n.baselineActive {
		return sub(c.RequeueCount, n.baseRequeue[c.Topic+"/"+c.Channel])
	}
	return c.RequeueCount
}

func (n *NSQTop) dispTopicMsgs(t *TopicData) int64 {
	if n.baselineActive {
		return sub(t.MessageCount, n.baseTopicMsgs[t.Topic])
	}
	return t.MessageCount
}

// sub returns cur-base, clamped at 0 so a counter reset (or a channel that
// appeared after the baseline) never shows a negative delta.
func sub(cur, base int64) int64 {
	if d := cur - base; d > 0 {
		return d
	}
	return 0
}

// sortChannels orders channels in place by the active sort column and
// direction, with a stable secondary sort on topic/channel name so equal-valued
// rows keep a consistent order across refreshes instead of jumping around.
func (n *NSQTop) sortChannels(channels []*ChannelData) {
	name := func(c *ChannelData) string { return c.Topic + "/" + c.Channel }

	// primaryCmp returns -1/0/1 comparing the active column in ascending order.
	primaryCmp := func(a, b *ChannelData) int {
		switch n.sortColumn {
		case 0:
			return strings.Compare(name(a), name(b))
		case 2:
			return cmpInt(int64(a.InFlightCount), int64(b.InFlightCount))
		case 3:
			return cmpInt(a.ReadyCount, b.ReadyCount)
		case 4:
			return cmpFloat(a.IncomingPerSecond, b.IncomingPerSecond)
		case 5:
			return cmpFloat(a.IncomingPerMinute, b.IncomingPerMinute)
		case 6:
			return cmpInt(n.dispProcessed(a), n.dispProcessed(b))
		case 7:
			return cmpInt(n.dispTimeout(a), n.dispTimeout(b))
		case 8:
			return cmpInt(n.dispRequeue(a), n.dispRequeue(b))
		default: // 1: Depth
			return cmpInt(int64(a.Depth), int64(b.Depth))
		}
	}

	sort.Slice(channels, func(i, j int) bool {
		a, b := channels[i], channels[j]
		if c := primaryCmp(a, b); c != 0 {
			if n.sortDesc {
				return c > 0
			}
			return c < 0
		}
		// Tiebreak by name, always ascending, for a stable order.
		return name(a) < name(b)
	})
}

// sortTopics orders topics in place by the topic view's active sort column,
// using the topic name as a stable secondary key.
func (n *NSQTop) sortTopics(topics []*TopicData) {
	primaryCmp := func(a, b *TopicData) int {
		switch n.sortColumn {
		case 0:
			return strings.Compare(a.Topic, b.Topic)
		case 1:
			return cmpInt(int64(a.ChannelCount), int64(b.ChannelCount))
		case 2:
			return cmpInt(int64(a.ConnectionCount), int64(b.ConnectionCount))
		case 4:
			return cmpInt(int64(a.InFlightCount), int64(b.InFlightCount))
		case 5:
			return cmpInt(a.ReadyCount, b.ReadyCount)
		case 6:
			return cmpFloat(a.IncomingPerSecond, b.IncomingPerSecond)
		case 7:
			return cmpFloat(a.IncomingPerMinute, b.IncomingPerMinute)
		case 8:
			return cmpInt(n.dispTopicMsgs(a), n.dispTopicMsgs(b))
		default: // 3: Depth
			return cmpInt(int64(a.Depth), int64(b.Depth))
		}
	}

	sort.Slice(topics, func(i, j int) bool {
		a, b := topics[i], topics[j]
		if c := primaryCmp(a, b); c != 0 {
			if n.sortDesc {
				return c > 0
			}
			return c < 0
		}
		return a.Topic < b.Topic
	})
}

// filterTopics returns topics whose name contains query (case-insensitive).
func filterTopics(topics []*TopicData, query string) []*TopicData {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return topics
	}
	var out []*TopicData
	for _, t := range topics {
		if strings.Contains(strings.ToLower(t.Topic), query) {
			out = append(out, t)
		}
	}
	return out
}

// TopicNodeRow is one nsqd node's contribution to a topic, shown in the topic
// detail view so you can see where the messages actually live.
type TopicNodeRow struct {
	NodeURL       string
	Display       string // node label (host:port)
	ChannelCount  int
	Depth         int
	InFlightCount int
	ClientCount   int
	ReadyCount    int64
	MessageCount  int64 // topic's produced-message counter on this node
}

// ClientRow is one consumer connection on a specific channel, shown in the
// channel detail view.
type ClientRow struct {
	NodeURL       string
	ClientID      string
	Hostname      string
	RemoteAddress string
	State         int
	ReadyCount    int64
	InFlightCount int64
	MessageCount  int64
	FinishCount   int64
	RequeueCount  int64
	ConnectTs     int64
}

// aggregateTopicDetail builds one row per nsqd node that hosts the named topic,
// summing the topic's channels' stats on that node.
func aggregateTopicDetail(raw []nodeStats, topicName string) []*TopicNodeRow {
	var rows []*TopicNodeRow
	for _, node := range raw {
		for _, topic := range node.Stats.Topics {
			if topic.TopicName != topicName {
				continue
			}
			row := &TopicNodeRow{
				NodeURL:      node.URL,
				Display:      displayNodeURL(node.URL),
				ChannelCount: len(topic.Channels),
				MessageCount: topic.MessageCount,
			}
			for _, ch := range topic.Channels {
				row.Depth += ch.Depth + ch.BackendDepth
				row.InFlightCount += ch.InFlightCount
				row.ClientCount += len(ch.Clients)
				for _, c := range ch.Clients {
					row.ReadyCount += c.ReadyCount
				}
			}
			rows = append(rows, row)
			break // a topic only appears once per node
		}
	}
	return rows
}

// aggregateChannelDetail builds one row per connected consumer on the named
// topic/channel across all nsqd nodes.
func aggregateChannelDetail(raw []nodeStats, target string) []*ClientRow {
	topicName, channelName, ok := splitTopicChannel(target)
	if !ok {
		return nil
	}
	var rows []*ClientRow
	for _, node := range raw {
		for _, topic := range node.Stats.Topics {
			if topic.TopicName != topicName {
				continue
			}
			for _, ch := range topic.Channels {
				if ch.ChannelName != channelName {
					continue
				}
				for _, c := range ch.Clients {
					rows = append(rows, &ClientRow{
						NodeURL:       node.URL,
						ClientID:      c.ClientID,
						Hostname:      c.Hostname,
						RemoteAddress: c.RemoteAddress,
						State:         c.State,
						ReadyCount:    c.ReadyCount,
						InFlightCount: c.InFlightCount,
						MessageCount:  c.MessageCount,
						FinishCount:   c.FinishCount,
						RequeueCount:  c.RequeueCount,
						ConnectTs:     c.ConnectTs,
					})
				}
			}
		}
	}
	return rows
}

// splitTopicChannel splits "topic/channel" into its parts. Topic and channel
// names can't contain "/", so a simple split is unambiguous.
func splitTopicChannel(key string) (topic, channel string, ok bool) {
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// displayNodeURL strips the scheme from an nsqd URL for compact display.
func displayNodeURL(u string) string {
	return strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://")
}

// sortTopicDetail orders per-node rows by the active sort column.
func (n *NSQTop) sortTopicDetail(rows []*TopicNodeRow) {
	primaryCmp := func(a, b *TopicNodeRow) int {
		switch n.sortColumn {
		case 0:
			return strings.Compare(a.Display, b.Display)
		case 1:
			return cmpInt(int64(a.ChannelCount), int64(b.ChannelCount))
		case 3:
			return cmpInt(int64(a.InFlightCount), int64(b.InFlightCount))
		case 4:
			return cmpInt(int64(a.ClientCount), int64(b.ClientCount))
		case 5:
			return cmpInt(a.ReadyCount, b.ReadyCount)
		case 6:
			return cmpInt(a.MessageCount, b.MessageCount)
		default: // 2: Depth
			return cmpInt(int64(a.Depth), int64(b.Depth))
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if c := primaryCmp(a, b); c != 0 {
			if n.sortDesc {
				return c > 0
			}
			return c < 0
		}
		return a.Display < b.Display
	})
}

// sortClientDetail orders per-client rows by the active sort column.
func (n *NSQTop) sortClientDetail(rows []*ClientRow) {
	label := func(c *ClientRow) string {
		if c.ClientID != "" {
			return c.ClientID
		}
		return c.RemoteAddress
	}
	primaryCmp := func(a, b *ClientRow) int {
		switch n.sortColumn {
		case 0:
			return strings.Compare(label(a), label(b))
		case 1:
			return strings.Compare(a.Hostname, b.Hostname)
		case 2:
			return strings.Compare(a.RemoteAddress, b.RemoteAddress)
		case 3:
			return cmpInt(int64(a.State), int64(b.State))
		case 4:
			return cmpInt(a.ReadyCount, b.ReadyCount)
		case 6:
			return cmpInt(a.MessageCount, b.MessageCount)
		case 7:
			return cmpInt(a.RequeueCount, b.RequeueCount)
		case 8:
			return cmpInt(a.ConnectTs, b.ConnectTs)
		default: // 5: In-Flight
			return cmpInt(a.InFlightCount, b.InFlightCount)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if c := primaryCmp(a, b); c != 0 {
			if n.sortDesc {
				return c > 0
			}
			return c < 0
		}
		return label(a) < label(b)
	})
}

// clientStateLabel renders nsqd's client State int as a short word. The values
// are from nsqd's protocol: init/disconnected/connected/subscribed/closing.
func clientStateLabel(s int) string {
	switch s {
	case 0:
		return "Init"
	case 1:
		return "Disconn"
	case 2:
		return "Conn"
	case 3:
		return "Sub"
	case 4:
		return "Closing"
	default:
		return strconv.Itoa(s)
	}
}

// formatConnectedFor renders an nsqd connect_ts (seconds since epoch) as a
// short "how long has this client been connected" duration like "3m12s".
func formatConnectedFor(connectTs int64) string {
	if connectTs <= 0 {
		return "—"
	}
	d := time.Since(time.Unix(connectTs, 0))
	if d < 0 {
		return "—"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd%02dh", int(d.Hours())/24, int(d.Hours())%24)
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func (n *NSQTop) startMonitoring() {
	go func() {
		ticker := time.NewTicker(n.getInterval())
		defer ticker.Stop()

		// Initial update
		n.updateData()

		for {
			select {
			case <-ticker.C:
				n.updateData()
			case d := <-n.intervalCh:
				ticker.Reset(d)
			}
		}
	}()
}

func (n *NSQTop) updateData() {
	// When nsqd addresses are given explicitly, query them directly and skip
	// nsqlookupd discovery.
	nodeURLs := n.nsqdURLs
	if len(nodeURLs) == 0 {
		discovered, err := n.getNSQDNodes()
		if err != nil {
			n.app.QueueUpdateDraw(func() {
				n.summary.SetText(fmt.Sprintf("[#f7768e]Error: %s[-]", err.Error()))
			})
			return
		}
		nodeURLs = discovered
	}

	allStats, err := n.getAllStats(nodeURLs)
	if err != nil {
		n.app.QueueUpdateDraw(func() {
			n.summary.SetText(fmt.Sprintf("[#f7768e]Error: %s[-]", err.Error()))
		})
		return
	}

	channels, topics, totals := n.processStats(allStats)

	// Trend sample = messages processed during this interval + current in-flight,
	// so the graph reflects actual traffic rather than just the in-flight gauge
	// (which stays near zero when consumers keep up).
	processedThisInterval := int64(0)
	if n.havePrevProcessed {
		if d := totals.Processed - n.prevProcessed; d > 0 { // ignore counter resets
			processedThisInterval = d
		}
	}
	n.prevProcessed = totals.Processed
	n.havePrevProcessed = true

	n.trendHistory = append(n.trendHistory, totals.Inflight+int(processedThisInterval))
	if len(n.trendHistory) > MaxHistory {
		n.trendHistory = n.trendHistory[len(n.trendHistory)-MaxHistory:]
	}

	n.app.QueueUpdateDraw(func() {
		n.lastRaw = allStats
		n.updateUI(channels, topics, totals, nodeURLs)
	})
}

// getNSQDNodes discovers nsqd HTTP base URLs (e.g. "http://host:4151") via the
// configured nsqlookupd instances.
func (n *NSQTop) getNSQDNodes() ([]string, error) {
	var errors []string
	seen := make(map[string]bool)
	var nodeURLs []string

	for _, lookupURL := range n.lookupURLs {
		resp, err := n.client.Get(lookupURL + "/nodes")
		if err != nil {
			errors = append(errors, fmt.Sprintf("Failed to connect to %s: %v", lookupURL, err))
			continue
		}

		var nodesResp NodesResponse
		err = json.NewDecoder(resp.Body).Decode(&nodesResp)
		resp.Body.Close()
		if err != nil {
			errors = append(errors, fmt.Sprintf("Invalid JSON from %s", lookupURL))
			continue
		}

		for _, producer := range nodesResp.Producers {
			url := fmt.Sprintf("http://%s:%d", producer.BroadcastAddress, producer.HTTPPort)
			if !seen[url] {
				seen[url] = true
				nodeURLs = append(nodeURLs, url)
			}
		}
	}

	if len(nodeURLs) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(errors, "; "))
	}

	return nodeURLs, nil
}

// nodeStats pairs a successful nsqd /stats response with the URL it came from
// so detail views can attribute rows back to a specific node.
type nodeStats struct {
	URL   string
	Stats StatsResponse
}

func (n *NSQTop) getAllStats(nodeURLs []string) ([]nodeStats, error) {
	var allStats []nodeStats

	for _, base := range nodeURLs {
		resp, err := n.client.Get(base + "/stats?format=json")
		if err != nil {
			continue // Ignore failed nodes
		}

		var stats StatsResponse
		err = json.NewDecoder(resp.Body).Decode(&stats)
		resp.Body.Close()
		if err != nil {
			continue
		}

		// Handle newer NSQ versions where data is nested
		if stats.Data != nil {
			allStats = append(allStats, nodeStats{URL: base, Stats: *stats.Data})
		} else {
			allStats = append(allStats, nodeStats{URL: base, Stats: stats})
		}
	}

	return allStats, nil
}

func (n *NSQTop) processStats(allStats []nodeStats) ([]*ChannelData, []*TopicData, Totals) {
	channelData := make(map[string]*ChannelData)
	topicMessages := make(map[string]int64)
	totalInflight := 0

	for _, node := range allStats {
		for _, topic := range node.Stats.Topics {
			topicMessages[topic.TopicName] += topic.MessageCount

			for _, channel := range topic.Channels {
				key := fmt.Sprintf("%s/%s", topic.TopicName, channel.ChannelName)

				if _, exists := channelData[key]; !exists {
					channelData[key] = &ChannelData{
						Topic:   topic.TopicName,
						Channel: channel.ChannelName,
					}
				}

				data := channelData[key]
				data.Depth += channel.Depth + channel.BackendDepth
				data.InFlightCount += channel.InFlightCount
				data.ClientCount += len(channel.Clients)
				for _, c := range channel.Clients {
					data.ReadyCount += c.ReadyCount
				}
				data.MessageCount += channel.MessageCount
				data.TimeoutCount += channel.TimeoutCount
				data.RequeueCount += channel.RequeueCount
				totalInflight += channel.InFlightCount
			}
		}
	}

	// Incoming rate comes from the topic's produced-message counter: every
	// message produced to a topic is copied to each of its channels, so the
	// topic's rate is the incoming rate for all of its channels. Timeout and
	// requeue growth are tracked per channel. All are smoothed (see smoothRates).
	timeoutCounts := make(map[string]int64, len(channelData))
	requeueCounts := make(map[string]int64, len(channelData))
	for key, data := range channelData {
		timeoutCounts[key] = data.TimeoutCount
		requeueCounts[key] = data.RequeueCount
	}

	topicIncoming := n.smoothRates(topicMessages, n.previousTopicStats, n.topicRateEMA)
	timeoutRates := n.smoothRates(timeoutCounts, n.prevTimeoutCount, n.timeoutRateEMA)
	requeueRates := n.smoothRates(requeueCounts, n.prevRequeueCount, n.requeueRateEMA)

	// Store current counts and smoothed rates for the next iteration.
	n.previousTopicStats, n.topicRateEMA = topicMessages, topicIncoming
	n.prevTimeoutCount, n.timeoutRateEMA = timeoutCounts, timeoutRates
	n.prevRequeueCount, n.requeueRateEMA = requeueCounts, requeueRates

	// Cluster-wide totals: sum produced-message counts and smoothed rates.
	totals := Totals{Inflight: totalInflight}
	for _, count := range topicMessages {
		totals.MessageCount += count
	}
	for _, data := range channelData {
		totals.Processed += data.MessageCount
	}
	for _, rate := range topicIncoming {
		totals.IncomingPerSec += rate
	}

	// Convert to slice and attach each channel's rates. Timeout/requeue counts
	// are nsqd's cumulative lifetime totals. Ordering is handled later in
	// updateUI based on the active sort column.
	var channels []*ChannelData
	for key, data := range channelData {
		data.IncomingPerSecond = topicIncoming[data.Topic]
		data.IncomingPerMinute = data.IncomingPerSecond * 60
		data.TimeoutRate = timeoutRates[key]
		data.RequeueRate = requeueRates[key]
		channels = append(channels, data)
	}

	// Per-topic rollups for the topic view.
	topicData := make(map[string]*TopicData)
	for _, data := range channelData {
		td, ok := topicData[data.Topic]
		if !ok {
			td = &TopicData{Topic: data.Topic}
			topicData[data.Topic] = td
		}
		td.ChannelCount++
		td.ConnectionCount += data.ClientCount
		td.ReadyCount += data.ReadyCount
		td.Depth += data.Depth
		td.InFlightCount += data.InFlightCount
	}
	for name, msgs := range topicMessages {
		td, ok := topicData[name]
		if !ok {
			// Topic exists with no channels yet — still worth showing.
			td = &TopicData{Topic: name}
			topicData[name] = td
		}
		td.MessageCount = msgs
		td.IncomingPerSecond = topicIncoming[name]
		td.IncomingPerMinute = td.IncomingPerSecond * 60
	}

	var topics []*TopicData
	for _, t := range topicData {
		topics = append(topics, t)
	}

	return channels, topics, totals
}

// smoothRates turns cumulative counters into a per-second growth rate, fed
// through an exponential moving average so the value is a stable running figure
// rather than a noisy per-sample reading. Idle counters decay toward (and read)
// 0; counter resets (negative deltas) are ignored. The returned map becomes the
// next iteration's EMA state.
func (n *NSQTop) smoothRates(current, previous map[string]int64, prevEMA map[string]float64) map[string]float64 {
	seconds := n.getInterval().Seconds()
	out := make(map[string]float64, len(current))
	for key, count := range current {
		instant := 0.0
		if prev, ok := previous[key]; ok {
			if diff := float64(count - prev); diff > 0 {
				instant = diff / seconds
			}
		}
		if ema, ok := prevEMA[key]; ok {
			out[key] = RateEMAAlpha*instant + (1-RateEMAAlpha)*ema
		} else {
			out[key] = instant
		}
	}
	return out
}

func (n *NSQTop) updateUI(channels []*ChannelData, topics []*TopicData, totals Totals, nodeURLs []string) {
	// Cluster-wide totals stay the same regardless of view.
	totalDepth := 0
	for _, channel := range channels {
		totalDepth += channel.Depth
	}

	lookupDisplay := strings.Join(n.lookupURLs, ", ")
	if len(n.lookupURLs) > 3 {
		lookupDisplay = fmt.Sprintf("%d servers", len(n.lookupURLs))
	}
	if len(n.lookupURLs) == 0 {
		lookupDisplay = "nsqd directly"
	}

	var nsqdServers []string
	for _, u := range nodeURLs {
		nsqdServers = append(nsqdServers, strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://"))
	}
	nsqdDisplay := strings.Join(nsqdServers, ", ")
	if len(nsqdServers) > 3 {
		nsqdDisplay = fmt.Sprintf("%d nsqd nodes", len(nsqdServers))
	}

	sortDirArrow := "▲"
	if n.sortDesc {
		sortDirArrow = "▼"
	}

	// Filter the displayed rows for the active view. Cluster totals above stay
	// global regardless of the filter. Detail views don't filter — the lists
	// are short and the drill target is already the scope.
	var displayChannels []*ChannelData
	var displayTopics []*TopicData
	var displayTopicDetail []*TopicNodeRow
	var displayClientDetail []*ClientRow
	var rowsLabel, rowsField string
	switch n.viewMode {
	case viewTopics:
		displayTopics = filterTopics(topics, n.filterText)
		rowsLabel = "Topics"
		rowsField = strconv.Itoa(len(topics))
		if n.filterText != "" {
			rowsField = fmt.Sprintf("%d/%d matching %q", len(displayTopics), len(topics), n.filterText)
		}
	case viewTopicDetail:
		displayTopicDetail = aggregateTopicDetail(n.lastRaw, n.drillTarget)
		rowsLabel = "Nodes"
		rowsField = strconv.Itoa(len(displayTopicDetail))
	case viewChannelDetail:
		displayClientDetail = aggregateChannelDetail(n.lastRaw, n.drillTarget)
		rowsLabel = "Clients"
		rowsField = strconv.Itoa(len(displayClientDetail))
	default: // viewChannels
		displayChannels = filterChannels(channels, n.filterText)
		rowsLabel = "Channels"
		rowsField = strconv.Itoa(len(channels))
		if n.filterText != "" {
			rowsField = fmt.Sprintf("%d/%d matching %q", len(displayChannels), len(channels), n.filterText)
		}
	}

	// When a baseline is active, Total Msgs and the cumulative columns read as a
	// delta since it was captured; flag this on the totals line.
	totalMsgs := totals.MessageCount
	msgsLabel := "Total Msgs"
	if n.baselineActive {
		totalMsgs = sub(totals.MessageCount, n.baseTotalMsgs)
		msgsLabel = fmt.Sprintf("Δ Msgs (since %s)", n.baselineAt.Format("15:04:05"))
	}
	clusterPrefix := ""
	if n.clusterName != "" {
		clusterPrefix = fmt.Sprintf("[%s] ", n.clusterName)
	}
	columns := n.activeColumns()
	summaryText := fmt.Sprintf(
		"[#7aa2f7]%sNSQ Top - %s - Connected to %s[-]\n"+
			"[#e0af68]Total Depth: %s | Total In-Flight: %s | %s: %s[-]\n"+
			"[#bb9af7]%s: %s | Rate: %s/s, %s/m[-]\n"+
			"[#9ece6a]NSQd Servers: %s[-]\n"+
			"[#565f89]Sort: %s %s  •  Refresh: %s  •  Tab cycle  •  Enter drill  •  Esc back  •  / filter  •  ←/→ col  •  Space rev  •  −/+ rate  •  c/C zero  •  ^C quit[-]",
		clusterPrefix,
		time.Now().Format("2006-01-02 15:04:05"),
		lookupDisplay,
		formatNumber(totalDepth),
		formatNumber(totals.Inflight),
		rowsLabel, rowsField,
		msgsLabel,
		formatNumber64(totalMsgs),
		formatRate(totals.IncomingPerSec, 1),
		formatRate(totals.IncomingPerSec*60, 0),
		nsqdDisplay,
		columns[n.sortColumn], sortDirArrow,
		formatInterval(n.getInterval()),
	)
	n.summary.SetText(summaryText)

	// Render the traffic trend across the full width of its panel.
	trendWidth := SparklineLength
	if _, _, w, _ := n.trend.GetInnerRect(); w > 0 {
		trendWidth = w
	}
	history := n.trendHistory
	if len(history) > trendWidth {
		history = history[len(history)-trendWidth:]
	}
	n.trend.SetText("[#7dcfff]" + generateSparkline(history) + "[-]")

	// Remember the full (unfiltered) snapshot so a key press can re-sort,
	// re-filter, or flip views between refresh ticks.
	n.lastChannels = channels
	n.lastTopics = topics
	n.lastTotals = totals
	n.lastNodes = nodeURLs

	n.table.Clear()

	// Headers shared between views: bold, underlined accent text. An arrow
	// marks the active sort column. Per-view alignment and delta-marking
	// happens via the helpers below.
	deltaCol := n.deltaColumns()
	for i, header := range columns {
		if n.baselineActive && deltaCol[i] {
			header = "Δ " + header
		}
		if i == n.sortColumn {
			header = header + " " + sortDirArrow
		}
		align := tview.AlignRight
		if n.columnLeftAligned(i) {
			align = tview.AlignLeft
		}
		cell := tview.NewTableCell(header).
			SetAlign(align).
			SetAttributes(tcell.AttrBold | tcell.AttrUnderline).
			SetTextColor(colorAccent).
			SetSelectable(false)
		// Tint the active sort column so the eye can find it at a glance,
		// alongside the existing ▲/▼ marker. Data cells get the same tint
		// below for one continuous stripe.
		if i == n.sortColumn {
			cell.SetBackgroundColor(colorColumnTint)
		}
		n.table.SetCell(0, i, cell)
	}

	switch n.viewMode {
	case viewTopics:
		n.sortTopics(displayTopics)
		n.renderTopicRows(displayTopics)
	case viewTopicDetail:
		n.sortTopicDetail(displayTopicDetail)
		n.renderTopicDetailRows(displayTopicDetail)
	case viewChannelDetail:
		n.sortClientDetail(displayClientDetail)
		n.renderClientDetailRows(displayClientDetail)
	default: // viewChannels
		n.sortChannels(displayChannels)
		n.renderChannelRows(displayChannels)
	}

	// Stripe the active sort column over the data rows so it reads as one
	// continuous tint top-to-bottom. Selected-row highlighting still wins at
	// the intersection, which is the behavior we want.
	for r := 1; r < n.table.GetRowCount(); r++ {
		if c := n.table.GetCell(r, n.sortColumn); c != nil {
			c.SetBackgroundColor(colorColumnTint)
		}
	}

	// Pin the header row so it stays visible while scrolling. Snap to the top
	// on first draw and whenever the sort changes, but otherwise leave the
	// user's scroll position alone.
	n.table.SetFixed(1, 0)
	if n.scrollTop {
		n.table.ScrollToBeginning()
		if n.table.GetRowCount() > 1 {
			n.table.Select(1, 0)
		}
		n.scrollTop = false
	}
}

// deltaColumns returns a map column-index -> true for the columns that
// represent cumulative counters; their headers gain a Δ prefix when a baseline
// is active.
func (n *NSQTop) deltaColumns() map[int]bool {
	switch n.viewMode {
	case viewTopics:
		return map[int]bool{8: true} // Messages
	case viewChannels:
		return map[int]bool{6: true, 7: true, 8: true} // Processed, Timeouts, Requeues
	default:
		// Detail views show point-in-time counters; baseline doesn't apply.
		return nil
	}
}

// columnLeftAligned reports whether the column at index i holds text (and
// should be left-aligned) rather than a number.
func (n *NSQTop) columnLeftAligned(i int) bool {
	if n.viewMode == viewChannelDetail {
		// Client | Hostname | Remote | State are text; everything else is numeric.
		return i <= 3
	}
	return i == 0
}

func (n *NSQTop) renderChannelRows(display []*ChannelData) {
	for i, channel := range display {
		row := i + 1
		topicChannel := fmt.Sprintf("%s/%s", channel.Topic, channel.Channel)
		n.table.SetCell(row, 0, tview.NewTableCell(topicChannel).SetReference(topicChannel))

		depthCell := tview.NewTableCell(formatNumber(channel.Depth)).SetAlign(tview.AlignRight)
		colorDepth(depthCell, channel.Depth)
		n.table.SetCell(row, 1, depthCell)

		n.table.SetCell(row, 2, tview.NewTableCell(formatNumber(channel.InFlightCount)).SetAlign(tview.AlignRight))

		// Ready: zero with active consumers is a "consumers paused / not asking
		// for more" signal — color it the same as a stalled rate.
		readyCell := tview.NewTableCell(formatNumber64(channel.ReadyCount)).SetAlign(tview.AlignRight)
		if channel.ReadyCount == 0 && channel.ClientCount > 0 {
			readyCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 3, readyCell)

		n.table.SetCell(row, 4, tview.NewTableCell(formatRate(channel.IncomingPerSecond, 1)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 5, tview.NewTableCell(formatRate(channel.IncomingPerMinute, 0)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 6, tview.NewTableCell(formatNumber64(n.dispProcessed(channel))).SetAlign(tview.AlignRight))

		timeoutCell := tview.NewTableCell(formatGrowth(n.dispTimeout(channel), channel.TimeoutRate)).SetAlign(tview.AlignRight)
		if channel.TimeoutRate >= 0.05 {
			timeoutCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 7, timeoutCell)

		requeueCell := tview.NewTableCell(formatGrowth(n.dispRequeue(channel), channel.RequeueRate)).SetAlign(tview.AlignRight)
		if channel.RequeueRate >= 0.05 {
			requeueCell.SetTextColor(colorWarn)
		}
		n.table.SetCell(row, 8, requeueCell)
	}
}

func (n *NSQTop) renderTopicRows(display []*TopicData) {
	for i, t := range display {
		row := i + 1
		n.table.SetCell(row, 0, tview.NewTableCell(t.Topic).SetReference(t.Topic))
		n.table.SetCell(row, 1, tview.NewTableCell(formatNumber(t.ChannelCount)).SetAlign(tview.AlignRight))

		// No connections is a "no consumers" signal; flag it the same way as a
		// high depth so a topic backing up with zero consumers stands out.
		connCell := tview.NewTableCell(formatNumber(t.ConnectionCount)).SetAlign(tview.AlignRight)
		if t.ConnectionCount == 0 && t.ChannelCount > 0 {
			connCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 2, connCell)

		depthCell := tview.NewTableCell(formatNumber(t.Depth)).SetAlign(tview.AlignRight)
		colorDepth(depthCell, t.Depth)
		n.table.SetCell(row, 3, depthCell)

		n.table.SetCell(row, 4, tview.NewTableCell(formatNumber(t.InFlightCount)).SetAlign(tview.AlignRight))

		readyCell := tview.NewTableCell(formatNumber64(t.ReadyCount)).SetAlign(tview.AlignRight)
		if t.ReadyCount == 0 && t.ConnectionCount > 0 {
			readyCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 5, readyCell)

		n.table.SetCell(row, 6, tview.NewTableCell(formatRate(t.IncomingPerSecond, 1)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 7, tview.NewTableCell(formatRate(t.IncomingPerMinute, 0)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 8, tview.NewTableCell(formatNumber64(n.dispTopicMsgs(t))).SetAlign(tview.AlignRight))
	}
}

// renderTopicDetailRows writes one row per nsqd node hosting the drilled-into
// topic.
func (n *NSQTop) renderTopicDetailRows(display []*TopicNodeRow) {
	for i, r := range display {
		row := i + 1
		n.table.SetCell(row, 0, tview.NewTableCell(r.Display))
		n.table.SetCell(row, 1, tview.NewTableCell(formatNumber(r.ChannelCount)).SetAlign(tview.AlignRight))

		depthCell := tview.NewTableCell(formatNumber(r.Depth)).SetAlign(tview.AlignRight)
		colorDepth(depthCell, r.Depth)
		n.table.SetCell(row, 2, depthCell)

		n.table.SetCell(row, 3, tview.NewTableCell(formatNumber(r.InFlightCount)).SetAlign(tview.AlignRight))

		connCell := tview.NewTableCell(formatNumber(r.ClientCount)).SetAlign(tview.AlignRight)
		if r.ClientCount == 0 && r.ChannelCount > 0 {
			connCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 4, connCell)

		readyCell := tview.NewTableCell(formatNumber64(r.ReadyCount)).SetAlign(tview.AlignRight)
		if r.ReadyCount == 0 && r.ClientCount > 0 {
			readyCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 5, readyCell)

		n.table.SetCell(row, 6, tview.NewTableCell(formatNumber64(r.MessageCount)).SetAlign(tview.AlignRight))
	}
}

// renderClientDetailRows writes one row per consumer connection on the
// drilled-into channel.
func (n *NSQTop) renderClientDetailRows(display []*ClientRow) {
	for i, c := range display {
		row := i + 1
		label := c.ClientID
		if label == "" {
			label = c.RemoteAddress
		}
		n.table.SetCell(row, 0, tview.NewTableCell(label))
		n.table.SetCell(row, 1, tview.NewTableCell(c.Hostname))
		n.table.SetCell(row, 2, tview.NewTableCell(c.RemoteAddress))

		stateCell := tview.NewTableCell(clientStateLabel(c.State))
		if c.State != 3 { // 3 = Subscribed; anything else means not actively consuming
			stateCell.SetTextColor(colorWarn)
		}
		n.table.SetCell(row, 3, stateCell)

		readyCell := tview.NewTableCell(formatNumber64(c.ReadyCount)).SetAlign(tview.AlignRight)
		if c.ReadyCount == 0 && c.State == 3 {
			readyCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 4, readyCell)

		n.table.SetCell(row, 5, tview.NewTableCell(formatNumber64(c.InFlightCount)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 6, tview.NewTableCell(formatNumber64(c.MessageCount)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 7, tview.NewTableCell(formatNumber64(c.RequeueCount)).SetAlign(tview.AlignRight))
		n.table.SetCell(row, 8, tview.NewTableCell(formatConnectedFor(c.ConnectTs)).SetAlign(tview.AlignRight))
	}
}

// colorDepth applies the standard depth color coding used in both views.
func colorDepth(cell *tview.TableCell, depth int) {
	switch {
	case depth >= DepthCritThreshold:
		cell.SetTextColor(colorCrit)
	case depth >= DepthWarnThreshold:
		cell.SetTextColor(colorWarn)
	default:
		cell.SetTextColor(colorOK)
	}
}

func generateSparkline(history []int) string {
	if len(history) == 0 {
		return ""
	}

	max := 0
	for _, val := range history {
		if val > max {
			max = val
		}
	}

	if max == 0 {
		max = 1
	}

	// Convert string to rune slice to handle Unicode characters properly
	sparklineRunes := []rune(SparklineChars)

	var result strings.Builder
	for _, val := range history {
		index := (val * (len(sparklineRunes) - 1)) / max
		if index >= len(sparklineRunes) {
			index = len(sparklineRunes) - 1
		}
		result.WriteRune(sparklineRunes[index])
	}

	return result.String()
}

func formatNumber(n int) string {
	return formatNumber64(int64(n))
}

func formatNumber64(n int64) string {
	str := strconv.FormatInt(n, 10)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, char := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(char)
	}

	return result.String()
}

// formatInterval renders a refresh interval as e.g. "200ms" or "5.0s".
func formatInterval(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// formatRate renders a smoothed rate, reading 0 (not blank) when idle. Tiny
// residual averages are clamped to 0 so a quiet channel doesn't linger at 0.0+.
func formatRate(rate float64, decimals int) string {
	if rate < 0.05 {
		rate = 0
	}
	return strconv.FormatFloat(rate, 'f', decimals, 64)
}

// formatGrowth renders a cumulative count, appending a "▲<rate>" marker (per
// second) when the counter is currently climbing.
func formatGrowth(count int64, rate float64) string {
	s := formatNumber64(count)
	if rate >= 0.05 {
		s += " ▲" + strconv.FormatFloat(rate, 'f', 1, 64)
	}
	return s
}

// filterChannels returns the channels whose "topic/channel" contains query
// (case-insensitive). An empty query returns the input unchanged.
func filterChannels(channels []*ChannelData, query string) []*ChannelData {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return channels
	}
	var out []*ChannelData
	for _, c := range channels {
		if strings.Contains(strings.ToLower(c.Topic+"/"+c.Channel), query) {
			out = append(out, c)
		}
	}
	return out
}

// normalizeAddresses splits a comma-separated list of host:port addresses,
// trims whitespace, ensures each has an http(s):// scheme, and drops any
// trailing slash so paths can be appended cleanly.
func normalizeAddresses(raw string) []string {
	var out []string
	for _, addr := range strings.Split(raw, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		out = append(out, strings.TrimRight(addr, "/"))
	}
	return out
}

func getEnvWithFallback(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvIntWithFallback(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return fallback
}
