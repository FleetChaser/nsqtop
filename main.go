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
	ChannelName   string `json:"channel_name"`
	Depth         int    `json:"depth"`
	BackendDepth  int    `json:"backend_depth"`
	InFlightCount int    `json:"in_flight_count"`
	MessageCount  int64  `json:"message_count"`
	TimeoutCount  int64  `json:"timeout_count"`
	RequeueCount  int64  `json:"requeue_count"`
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
	MessageCount      int64   // cumulative messages processed by the channel
	IncomingPerSecond float64 // rate of messages produced to the topic
	IncomingPerMinute float64
	TimeoutCount      int64
	RequeueCount      int64
	TimeoutRate       float64 // smoothed growth per second
	RequeueRate       float64
}

type NSQTop struct {
	app                *tview.Application
	table              *tview.Table
	summary            *tview.TextView
	trend              *tview.TextView
	filterInput        *tview.InputField
	flex               *tview.Flex
	client             *http.Client
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

	// Sorting state, driven by the arrow keys.
	sortColumn int
	sortDesc   bool

	// Last rendered snapshot, so a key press can re-sort without waiting for
	// the next refresh tick.
	lastChannels []*ChannelData
	lastTotals   Totals
	lastNodes    []string
}

// columnTitles is the table's column order; the sort column index refers to it.
var columnTitles = []string{"Topic/Channel", "Depth", "In-Flight", "In/sec", "In/min", "Processed", "Timeouts", "Requeues"}

const sortColumnDepth = 1

// CLI configuration
var (
	lookupAddresses string
	nsqdAddresses   string
	refreshInterval int
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

	rootCmd.Flags().StringVarP(&lookupAddresses, "lookupd-http-address", "l", defaultLookupd,
		"Comma-separated HTTP addresses of nsqlookupd instances (e.g., localhost:4161)")
	rootCmd.Flags().StringVarP(&nsqdAddresses, "nsqd-http-address", "n", defaultNSQD,
		"Comma-separated HTTP addresses of nsqd instances; queried directly, bypassing nsqlookupd (e.g., localhost:4151)")
	rootCmd.Flags().IntVarP(&refreshInterval, "interval", "i", defaultInterval,
		"Refresh interval in seconds")

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
		client:             &http.Client{Timeout: 5 * time.Second},
	}
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
	n.summary.SetBorder(true).SetTitle("NSQ Cluster Status")

	// Full-width in-flight trend strip, shown right above the table.
	n.trend = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(false)
	n.trend.SetBorder(true).SetTitle("Traffic Trend (processed + in-flight)")

	// Create table. No cell-border grid: rows stay single-spaced (denser), and
	// the header is distinguished by an underline instead (see updateUI). It
	// sits in its own bordered box with inner padding so the text isn't flush
	// against the edge, matching the panels above.
	n.table = tview.NewTable().
		SetBorders(false).
		SetSelectable(false, false)
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
			// Clear an active filter from the table view.
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
			n.sortDesc = !n.sortDesc
			n.scrollTop = true
			n.redraw()
			return nil
		case tcell.KeyRune:
			switch event.Rune() {
			case '/': // open the topic/channel filter
				n.startFilter()
				return nil
			case '-', '_': // smaller interval -> faster refresh
				n.adjustInterval(-IntervalStep)
				return nil
			case '+', '=': // larger interval -> slower refresh
				n.adjustInterval(IntervalStep)
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

// changeSortColumn moves the active sort column by delta (wrapping around) and
// resets to a sensible default direction: ascending for the name column,
// descending for the numeric ones.
func (n *NSQTop) changeSortColumn(delta int) {
	cols := len(columnTitles)
	n.sortColumn = (n.sortColumn + delta + cols) % cols
	n.sortDesc = n.sortColumn != 0
	n.scrollTop = true
	n.redraw()
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
	n.updateUI(n.lastChannels, n.lastTotals, n.lastNodes)
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
			return cmpFloat(a.IncomingPerSecond, b.IncomingPerSecond)
		case 4:
			return cmpFloat(a.IncomingPerMinute, b.IncomingPerMinute)
		case 5:
			return cmpInt(a.MessageCount, b.MessageCount)
		case 6:
			return cmpInt(a.TimeoutCount, b.TimeoutCount)
		case 7:
			return cmpInt(a.RequeueCount, b.RequeueCount)
		default: // Depth
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

	channels, totals := n.processStats(allStats)

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
		n.updateUI(channels, totals, nodeURLs)
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

func (n *NSQTop) getAllStats(nodeURLs []string) ([]StatsResponse, error) {
	var allStats []StatsResponse

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
			allStats = append(allStats, *stats.Data)
		} else {
			allStats = append(allStats, stats)
		}
	}

	return allStats, nil
}

func (n *NSQTop) processStats(allStats []StatsResponse) ([]*ChannelData, Totals) {
	channelData := make(map[string]*ChannelData)
	topicMessages := make(map[string]int64)
	totalInflight := 0

	for _, stats := range allStats {
		for _, topic := range stats.Topics {
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

	return channels, totals
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

func (n *NSQTop) updateUI(channels []*ChannelData, totals Totals, nodeURLs []string) {
	// Calculate total depth
	totalDepth := 0
	for _, channel := range channels {
		totalDepth += channel.Depth
	}

	// Update summary
	lookupDisplay := strings.Join(n.lookupURLs, ", ")
	if len(n.lookupURLs) > 3 {
		lookupDisplay = fmt.Sprintf("%d servers", len(n.lookupURLs))
	}
	if len(n.lookupURLs) == 0 {
		lookupDisplay = "nsqd directly"
	}

	// Format nsqd servers list (strip the scheme for display)
	var nsqdServers []string
	for _, u := range nodeURLs {
		nsqdServers = append(nsqdServers, strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://"))
	}
	nsqdDisplay := strings.Join(nsqdServers, ", ")
	if len(nsqdServers) > 3 {
		nsqdDisplay = fmt.Sprintf("%d nsqd nodes", len(nsqdServers))
	}

	// Apply the active substring filter to the displayed rows; cluster-wide
	// totals above stay global regardless of the filter.
	display := filterChannels(channels, n.filterText)
	channelsField := strconv.Itoa(len(channels))
	if n.filterText != "" {
		channelsField = fmt.Sprintf("%d/%d matching %q", len(display), len(channels), n.filterText)
	}

	sortDirArrow := "▲"
	if n.sortDesc {
		sortDirArrow = "▼"
	}
	summaryText := fmt.Sprintf(
		"[#7aa2f7]NSQ Top - %s - Connected to %s[-]\n"+
			"[#e0af68]Total Depth: %s | Total In-Flight: %s | Channels: %s[-]\n"+
			"[#bb9af7]Total Msgs: %s | Rate: %s/s, %s/m[-]\n"+
			"[#9ece6a]NSQd Servers: %s[-]\n"+
			"[#565f89]Sort: %s %s  •  Refresh: %s  •  / filter  •  ←/→ sort  •  Enter reverse  •  − faster / + slower  •  Ctrl+C quit[-]",
		time.Now().Format("2006-01-02 15:04:05"),
		lookupDisplay,
		formatNumber(totalDepth),
		formatNumber(totals.Inflight),
		channelsField,
		formatNumber64(totals.MessageCount),
		formatRate(totals.IncomingPerSec, 1),
		formatRate(totals.IncomingPerSec*60, 0),
		nsqdDisplay,
		columnTitles[n.sortColumn], sortDirArrow,
		formatInterval(n.getInterval()),
	)
	n.summary.SetText(summaryText)

	// Render the in-flight trend across the full width of its panel.
	trendWidth := SparklineLength
	if _, _, w, _ := n.trend.GetInnerRect(); w > 0 {
		trendWidth = w
	}
	history := n.trendHistory
	if len(history) > trendWidth {
		history = history[len(history)-trendWidth:]
	}
	n.trend.SetText("[#7dcfff]" + generateSparkline(history) + "[-]")

	// Sort the displayed rows by the active column/direction.
	n.sortChannels(display)

	// Remember the full (unfiltered) snapshot so a key press can re-sort or
	// re-filter between refresh ticks.
	n.lastChannels = channels
	n.lastTotals = totals
	n.lastNodes = nodeURLs

	// Update table
	n.table.Clear()

	// Headers: bold, underlined accent text (no grid). The first column is
	// left-aligned; numeric columns are right-aligned to sit over their values.
	// An arrow marks the active sort column.
	for i, header := range columnTitles {
		if i == n.sortColumn {
			header = header + " " + sortDirArrow
		}
		align := tview.AlignRight
		if i == 0 {
			align = tview.AlignLeft
		}
		cell := tview.NewTableCell(header).
			SetAlign(align).
			SetAttributes(tcell.AttrBold | tcell.AttrUnderline).
			SetTextColor(colorAccent).
			SetSelectable(false)
		n.table.SetCell(0, i, cell)
	}

	// Data rows
	for i, channel := range display {
		row := i + 1
		topicChannel := fmt.Sprintf("%s/%s", channel.Topic, channel.Channel)

		// Topic/Channel
		n.table.SetCell(row, 0, tview.NewTableCell(topicChannel))

		// Depth with color coding
		depthCell := tview.NewTableCell(formatNumber(channel.Depth)).SetAlign(tview.AlignRight)
		if channel.Depth >= DepthCritThreshold {
			depthCell.SetTextColor(colorCrit)
		} else if channel.Depth >= DepthWarnThreshold {
			depthCell.SetTextColor(colorWarn)
		} else {
			depthCell.SetTextColor(colorOK)
		}
		n.table.SetCell(row, 1, depthCell)

		// In-Flight
		n.table.SetCell(row, 2, tview.NewTableCell(formatNumber(channel.InFlightCount)).SetAlign(tview.AlignRight))

		// Incoming per second
		n.table.SetCell(row, 3, tview.NewTableCell(formatRate(channel.IncomingPerSecond, 1)).SetAlign(tview.AlignRight))

		// Incoming per minute
		n.table.SetCell(row, 4, tview.NewTableCell(formatRate(channel.IncomingPerMinute, 0)).SetAlign(tview.AlignRight))

		// Processed (cumulative messages handled by the channel)
		n.table.SetCell(row, 5, tview.NewTableCell(formatNumber64(channel.MessageCount)).SetAlign(tview.AlignRight))

		// Timeouts and Requeues, with a ▲rate growth marker when climbing.
		timeoutCell := tview.NewTableCell(formatGrowth(channel.TimeoutCount, channel.TimeoutRate)).SetAlign(tview.AlignRight)
		if channel.TimeoutRate >= 0.05 {
			timeoutCell.SetTextColor(colorCrit)
		}
		n.table.SetCell(row, 6, timeoutCell)

		requeueCell := tview.NewTableCell(formatGrowth(channel.RequeueCount, channel.RequeueRate)).SetAlign(tview.AlignRight)
		if channel.RequeueRate >= 0.05 {
			requeueCell.SetTextColor(colorWarn)
		}
		n.table.SetCell(row, 7, requeueCell)
	}

	// Pin the header row so it stays visible while scrolling. Snap to the top
	// on first draw and whenever the sort changes, but otherwise leave the
	// user's scroll position alone.
	n.table.SetFixed(1, 0)
	if n.scrollTop {
		n.table.ScrollToBeginning()
		n.scrollTop = false
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
