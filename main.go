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
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

// Configuration constants
const (
	SparklineChars      = " ▂▃▄▅▆▇█"
	SparklineLength     = 60
	DepthWarnThreshold  = 100
	DepthCritThreshold  = 1000
	DefaultInterval     = 2
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
	ChannelName     string `json:"channel_name"`
	Depth          int    `json:"depth"`
	BackendDepth   int    `json:"backend_depth"`
	InFlightCount  int    `json:"in_flight_count"`
	MessageCount   int64  `json:"message_count"`
}

type Topic struct {
	TopicName string    `json:"topic_name"`
	Channels  []Channel `json:"channels"`
}

type StatsResponse struct {
	Topics []Topic `json:"topics"`
	Data   *StatsResponse `json:"data,omitempty"` // For newer NSQ versions
}

type ChannelData struct {
	Topic           string
	Channel         string
	Depth          int
	InFlightCount  int
	MessageCount   int64
	RatePerSecond  float64
	RatePerMinute  float64
}

type NSQTop struct {
	app             *tview.Application
	table           *tview.Table
	summary         *tview.TextView
	lookupURLs      []string
	interval        time.Duration
	previousStats   map[string]*ChannelData
	inflightHistory []int
}

// CLI configuration
var (
	lookupAddresses string
	refreshInterval int
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "nsqtop",
		Short: "A top-like monitoring tool for NSQ clusters",
		Long:  "Monitor NSQ clusters in real-time with a terminal-based interface",
		Run:   runNSQTop,
	}

	// Get defaults from environment variables
	defaultLookupd := getEnvWithFallback("NSQTOP_LOOKUPD_ADDRESSES", 
		getEnvWithFallback("NSQTOP_LOOKUPD_ADDRESS", ""))
	defaultInterval := getEnvIntWithFallback("NSQTOP_INTERVAL", DefaultInterval)

	rootCmd.Flags().StringVarP(&lookupAddresses, "lookupd-http-address", "l", defaultLookupd,
		"Comma-separated HTTP addresses of nsqlookupd instances (e.g., localhost:4161)")
	rootCmd.Flags().IntVarP(&refreshInterval, "interval", "i", defaultInterval,
		"Refresh interval in seconds")

	rootCmd.MarkFlagRequired("lookupd-http-address")

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func runNSQTop(cmd *cobra.Command, args []string) {
	if lookupAddresses == "" {
		log.Fatal("--lookupd-http-address is required")
	}

	// Parse and normalize URLs
	urls := strings.Split(lookupAddresses, ",")
	var normalizedURLs []string
	for _, url := range urls {
		url = strings.TrimSpace(url)
		if url == "" {
			continue
		}
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			url = "http://" + url
		}
		normalizedURLs = append(normalizedURLs, url)
	}

	if len(normalizedURLs) == 0 {
		log.Fatal("At least one valid lookupd address is required")
	}

	nsqTop := &NSQTop{
		lookupURLs:      normalizedURLs,
		interval:        time.Duration(refreshInterval) * time.Second,
		previousStats:   make(map[string]*ChannelData),
		inflightHistory: make([]int, 0, SparklineLength),
	}

	nsqTop.initUI()
	nsqTop.startMonitoring()

	if err := nsqTop.app.Run(); err != nil {
		log.Fatal(err)
	}
}

func (n *NSQTop) initUI() {
	n.app = tview.NewApplication()

	// Create summary view
	n.summary = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(false)
	n.summary.SetBorder(true).SetTitle("NSQ Cluster Status")

	// Create table
	n.table = tview.NewTable().
		SetBorders(true).
		SetSelectable(false, false)

	// Set up layout
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(n.summary, 5, 1, false).
		AddItem(n.table, 0, 1, true)

	n.app.SetRoot(flex, true).SetFocus(flex)

	// Handle Ctrl+C
	n.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlC {
			n.app.Stop()
		}
		return event
	})
}

func (n *NSQTop) startMonitoring() {
	go func() {
		ticker := time.NewTicker(n.interval)
		defer ticker.Stop()

		// Initial update
		n.updateData()

		for range ticker.C {
			n.updateData()
		}
	}()
}

func (n *NSQTop) updateData() {
	nodes, err := n.getNSQDNodes()
	if err != nil {
		n.app.QueueUpdateDraw(func() {
			n.summary.SetText(fmt.Sprintf("[red]Error: %s[white]", err.Error()))
		})
		return
	}

	allStats, err := n.getAllStats(nodes)
	if err != nil {
		n.app.QueueUpdateDraw(func() {
			n.summary.SetText(fmt.Sprintf("[red]Error: %s[white]", err.Error()))
		})
		return
	}

	channels, totalInflight := n.processStats(allStats)

	// Update inflight history
	n.inflightHistory = append(n.inflightHistory, totalInflight)
	if len(n.inflightHistory) > SparklineLength {
		n.inflightHistory = n.inflightHistory[1:]
	}

	n.app.QueueUpdateDraw(func() {
		n.updateUI(channels, totalInflight, nodes)
	})
}

func (n *NSQTop) getNSQDNodes() ([]Producer, error) {
	var allProducers []Producer
	var errors []string

	for _, lookupURL := range n.lookupURLs {
		resp, err := http.Get(lookupURL + "/nodes")
		if err != nil {
			errors = append(errors, fmt.Sprintf("Failed to connect to %s: %v", lookupURL, err))
			continue
		}
		defer resp.Body.Close()

		var nodesResp NodesResponse
		if err := json.NewDecoder(resp.Body).Decode(&nodesResp); err != nil {
			errors = append(errors, fmt.Sprintf("Invalid JSON from %s", lookupURL))
			continue
		}

		allProducers = append(allProducers, nodesResp.Producers...)
	}

	if len(allProducers) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(errors, "; "))
	}

	// Remove duplicates
	seen := make(map[string]bool)
	var uniqueProducers []Producer
	for _, producer := range allProducers {
		key := fmt.Sprintf("%s:%d", producer.BroadcastAddress, producer.HTTPPort)
		if !seen[key] {
			seen[key] = true
			uniqueProducers = append(uniqueProducers, producer)
		}
	}

	return uniqueProducers, nil
}

func (n *NSQTop) getAllStats(nodes []Producer) ([]StatsResponse, error) {
	var allStats []StatsResponse

	for _, producer := range nodes {
		url := fmt.Sprintf("http://%s:%d/stats?format=json", 
			producer.BroadcastAddress, producer.HTTPPort)
		
		resp, err := http.Get(url)
		if err != nil {
			continue // Ignore failed nodes
		}
		defer resp.Body.Close()

		var stats StatsResponse
		if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
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

func (n *NSQTop) processStats(allStats []StatsResponse) ([]*ChannelData, int) {
	channelData := make(map[string]*ChannelData)
	totalInflight := 0

	for _, stats := range allStats {
		for _, topic := range stats.Topics {
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
				totalInflight += channel.InFlightCount
			}
		}
	}

	// Calculate rates
	for key, data := range channelData {
		if prevData, exists := n.previousStats[key]; exists {
			messagesDiff := float64(data.MessageCount - prevData.MessageCount)
			if messagesDiff > 0 {
				data.RatePerSecond = messagesDiff / n.interval.Seconds()
				data.RatePerMinute = data.RatePerSecond * 60
			}
		}
	}

	// Store current stats for next iteration
	n.previousStats = channelData

	// Convert to slice and sort by depth
	var channels []*ChannelData
	for _, data := range channelData {
		channels = append(channels, data)
	}

	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Depth > channels[j].Depth
	})

	return channels, totalInflight
}

func (n *NSQTop) updateUI(channels []*ChannelData, totalInflight int, nodes []Producer) {
	// Calculate total depth
	totalDepth := 0
	for _, channel := range channels {
		totalDepth += channel.Depth
	}

	// Update summary
	sparkline := generateSparkline(n.inflightHistory)
	lookupDisplay := strings.Join(n.lookupURLs, ", ")
	if len(n.lookupURLs) > 3 {
		lookupDisplay = fmt.Sprintf("%d servers", len(n.lookupURLs))
	}
	
	// Format nsqd servers list
	var nsqdServers []string
	for _, node := range nodes {
		nsqdServers = append(nsqdServers, fmt.Sprintf("%s:%d", node.BroadcastAddress, node.HTTPPort))
	}
	nsqdDisplay := strings.Join(nsqdServers, ", ")
	if len(nsqdServers) > 3 {
		nsqdDisplay = fmt.Sprintf("%d nsqd nodes", len(nsqdServers))
	}
	
	summaryText := fmt.Sprintf(
		"[blue]NSQ Top - %s - Connected to %s[white]\n"+
		"[yellow]Total Depth: %s | Total In-Flight: %s | Channels: %d | Trend: %s[white]\n"+
		"[green]NSQd Servers: %s[white]",
		time.Now().Format("2006-01-02 15:04:05"),
		lookupDisplay,
		formatNumber(totalDepth),
		formatNumber(totalInflight),
		len(channels),
		sparkline,
		nsqdDisplay,
	)
	n.summary.SetText(summaryText)

	// Update table
	n.table.Clear()

	// Headers
	headers := []string{"Topic/Channel", "Depth", "In-Flight", "Rate/sec", "Rate/min"}
	for i, header := range headers {
		cell := tview.NewTableCell(header).
			SetAlign(tview.AlignCenter).
			SetAttributes(tcell.AttrBold).
			SetBackgroundColor(tcell.ColorDarkBlue)
		n.table.SetCell(0, i, cell)
	}

	// Data rows
	for i, channel := range channels {
		row := i + 1
		topicChannel := fmt.Sprintf("%s/%s", channel.Topic, channel.Channel)

		// Topic/Channel
		n.table.SetCell(row, 0, tview.NewTableCell(topicChannel))

		// Depth with color coding
		depthCell := tview.NewTableCell(formatNumber(channel.Depth)).SetAlign(tview.AlignRight)
		if channel.Depth >= DepthCritThreshold {
			depthCell.SetTextColor(tcell.ColorRed)
		} else if channel.Depth >= DepthWarnThreshold {
			depthCell.SetTextColor(tcell.ColorYellow)
		} else {
			depthCell.SetTextColor(tcell.ColorGreen)
		}
		n.table.SetCell(row, 1, depthCell)

		// In-Flight
		n.table.SetCell(row, 2, tview.NewTableCell(formatNumber(channel.InFlightCount)).SetAlign(tview.AlignRight))

		// Rate/sec
		var rateSecText string
		if channel.RatePerSecond > 0 {
			rateSecText = fmt.Sprintf("%.1f", channel.RatePerSecond)
		} else {
			rateSecText = "--"
		}
		n.table.SetCell(row, 3, tview.NewTableCell(rateSecText).SetAlign(tview.AlignRight))

		// Rate/min
		var rateMinText string
		if channel.RatePerMinute > 0 {
			rateMinText = fmt.Sprintf("%.0f", channel.RatePerMinute)
		} else {
			rateMinText = "--"
		}
		n.table.SetCell(row, 4, tview.NewTableCell(rateMinText).SetAlign(tview.AlignRight))
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
	str := strconv.Itoa(n)
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
