// chart_manager.js - Production ready chart manager

class ChartManager {
    constructor() {
        this.charts = {};
        this.config = null;
        this.logger = console;
    }

    // Helper function to get timeframe duration in seconds
    getTimeframeDurationSeconds(timeframe) {
        const timeframeMap = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400,
            '1w': 604800
        };
        return timeframeMap[timeframe] || 0;
    }

    // Helper function to get candle close timestamp
    getCandleCloseTime(openTimestamp, timeframe) {
        const duration = this.getTimeframeDurationSeconds(timeframe);
        return openTimestamp + duration;
    }

    async loadConfiguration() {
        // Configuration loaded from API or defaults
        this.config = {
            chart: {
                height: 600,  // matches #chart CSS height
                background_color: '#1e222d',
                text_color: '#d1d4dc',
                grid_color: '#2a2e39',
                crosshair_color: '#758696',
                border_color: '#2a2e39'
            },
            candlestick: {
                up_color: '#26a69a',
                down_color: '#ef5350'
            },
            volume: {
                up_color: 'rgba(38, 166, 154, 0.5)',
                down_color: 'rgba(239, 83, 80, 0.5)',
                scale_top: 0.8,
                scale_bottom: 0.0
            },
            api: {
                base_url: '/api'
            }
        };
        
        return this.config;
    }

    async loadAllChartData(exchange, symbol, timeframe, fromYear = 2023) {
        // Load all chart data without limit from specified year
        if (!exchange || !symbol || !timeframe) {
            throw new Error('Missing required parameters: exchange, symbol, timeframe');
        }

        const url = `${this.config.api.base_url}/data/all?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&from_year=${fromYear}`;
        
        
        try {
            const response = await fetch(url);
            
            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }
            
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            return data;
            
        } catch (error) {
            throw error;
        }
    }

    createChart(containerId, symbol, timeframe = '1m') {
        console.log(`Creating chart for container: ${containerId}`);
        
        // Destroy existing chart if it exists to prevent race conditions
        if (this.charts[containerId]) {
            console.log(`Destroying existing chart for ${containerId}`);
            this.destroy(containerId);
        }
        
        const container = document.getElementById(containerId);
        if (!container) {
            throw new Error(`Container ${containerId} not found`);
        }

        if (typeof LightweightCharts === 'undefined') {
            throw new Error('LightweightCharts library is not loaded');
        }

        // Clear container
        container.innerHTML = '';

        try {
            
            // Force proper width calculation - use full available width
            const containerWidth = container.clientWidth || container.offsetWidth || window.innerWidth - 40;
            const actualWidth = containerWidth > 0 ? containerWidth : window.innerWidth - 40;
            
            const chart = LightweightCharts.createChart(container, {
                width: actualWidth,
                height: this.config.chart.height || 500,
                layout: {
                    backgroundColor: this.config.chart.background_color,
                    textColor: this.config.chart.text_color,
                },
                grid: {
                    vertLines: { color: this.config.chart.grid_color },
                    horzLines: { color: this.config.chart.grid_color },
                },
                crosshair: {
                    mode: LightweightCharts.CrosshairMode.Normal,
                    vertLine: {
                        width: 1,
                        color: this.config.chart.crosshair_color || '#758696',
                        style: LightweightCharts.LineStyle.Solid,
                        visible: true,
                        labelVisible: true,
                    },
                    horzLine: {
                        width: 1,
                        color: this.config.chart.crosshair_color || '#758696',
                        style: LightweightCharts.LineStyle.Solid,
                        visible: true,
                        labelVisible: true,
                    },
                },
                rightPriceScale: {
                    borderColor: this.config.chart.border_color,
                    visible: true,
                    borderVisible: true,
                },
                timeScale: {
                    borderColor: this.config.chart.border_color,
                    timeVisible: true,
                    secondsVisible: false,
                    shiftVisibleRangeOnNewBar: true,
                    fixLeftEdge: true,
                    fixRightEdge: true,
                    lockVisibleTimeRangeOnResize: true,
                    rightBarStaysOnScroll: true,
                    borderVisible: true,
                    visible: true,
                },
                localization: {
                    timeFormatter: (time) => {
                        try {
                            const date = new Date(time * 1000);
                            if (isNaN(date.getTime())) {
                                return 'Invalid Date';
                            }
                            const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
                            const day = date.getUTCDate().toString().padStart(2, '0');
                            const hours = date.getUTCHours().toString().padStart(2, '0');
                            const minutes = date.getUTCMinutes().toString().padStart(2, '0');
                            const seconds = date.getUTCSeconds().toString().padStart(2, '0');
                            return `${month}-${day} ${hours}:${minutes}:${seconds}`;
                        } catch (error) {
                            console.error('Time formatting error:', error);
                            return 'Time Error';
                        }
                    },
                },
            });

            const candlestickSeries = chart.addCandlestickSeries({
                upColor: this.config.candlestick.up_color,
                downColor: this.config.candlestick.down_color,
                borderVisible: false,
                wickUpColor: this.config.candlestick.up_color,
                wickDownColor: this.config.candlestick.down_color,
            });

            // Keep candlestick in top 80% so volume has room at the bottom
            chart.priceScale('right').applyOptions({
                scaleMargins: { top: 0.0, bottom: 0.2 },
            });

            const volumeSeries = chart.addHistogramSeries({
                color: '#26a69a',
                priceFormat: { type: 'volume' },
                priceScaleId: 'volume',
            });

            chart.priceScale('volume').applyOptions({
                scaleMargins: {
                    top: this.config.volume.scale_top,
                    bottom: this.config.volume.scale_bottom,
                },
            });

            this.charts[containerId] = {
                chart,
                candlestickSeries,
                volumeSeries,
                symbol: symbol || 'UNKNOWN',
                currentTimeframe: timeframe,
                volumeVisible: true,
                indicators: {}
            };

            console.log(`Chart created successfully for ${containerId}`);
            return this.charts[containerId];

        } catch (error) {
            console.error('Error creating chart:', error);
            throw error;
        }
    }

    async loadChartData(exchange, symbol, timeframe, options = {}) {
        console.log(`DEBUG: loadChartData called with exchange=${exchange}, symbol=${symbol}, timeframe=${timeframe}`);
        
        if (!exchange || !symbol || !timeframe) {
            throw new Error('Missing required parameters: exchange, symbol, timeframe');
        }

        const preferAggregated = options.prefer_aggregated !== undefined ? options.prefer_aggregated : true;
        const limit = options.limit || 500;
        const loadAll = options.loadAll !== undefined ? options.loadAll : true;  // Default to loading all historical data
        const fromYear = options.fromYear || null;  // New option to load from specific year

        let url = `${this.config.api.base_url}/chart?exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}`;
        
        url += `&limit=${limit}`;
        
        console.log('Loading chart data from:', url);
        
        try {
            const response = await fetch(url);
            
            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }
            
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            console.log('Chart data loaded:', data);
            return data;
            
        } catch (error) {
            console.error('Failed to load chart data:', error);
            throw error;
        }
    }

    updateChart(containerId, data) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        if (!data || !data.data) {
            throw new Error('No data in response');
        }

        // Convert chart data to the expected format
        const dataset = {
            candles: data.data,
            count: data.count,
            exchange: data.exchange,
            symbol: data.symbol,
            timeframe: data.timeframe
        };
        if (!dataset.candles || !Array.isArray(dataset.candles)) {
            throw new Error('No valid candles array in dataset');
        }

        console.log(`Processing ${dataset.candles.length} candles`);

        try {
            // Validate and format candle data
            
            const validCandles = dataset.candles.filter(candle => {
                if (!candle) {
                    console.log('Invalid candle: null/undefined');
                    return false;
                }
                
                const checks = {
                    timestamp: typeof candle.timestamp === 'number',
                    open: typeof candle.open === 'number',
                    high: typeof candle.high === 'number', 
                    low: typeof candle.low === 'number',
                    close: typeof candle.close === 'number',
                    volume: typeof candle.volume === 'number',
                    ohlcValid: !isNaN(candle.open) && !isNaN(candle.high) && !isNaN(candle.low) && !isNaN(candle.close),
                    highLowValid: candle.high >= candle.low,
                    highMaxValid: candle.high >= Math.max(candle.open, candle.close),
                    lowMinValid: candle.low <= Math.min(candle.open, candle.close),
                    positiveValues: candle.open > 0 && candle.high > 0 && candle.low > 0 && candle.close > 0
                };
                
                const isValid = Object.values(checks).every(check => check === true);
                
                if (!isValid) {
                    console.log('Invalid candle found:', candle, 'Failed checks:', checks);
                }
                
                return isValid;
            });

            if (validCandles.length === 0) {
                throw new Error('No valid candles after filtering');
            }


            const candleData = validCandles.map(candle => {
                const open = parseFloat(candle.open);
                const high = parseFloat(candle.high); 
                const low = parseFloat(candle.low);
                const close = parseFloat(candle.close);
                const time = candle.timestamp;
                
                // Ensure all values are valid finite numbers
                if (!Number.isFinite(time) || !Number.isFinite(open) || 
                    !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) {
                    console.error('Non-finite values in candle:', { time, open, high, low, close });
                    return null;
                }
                
                // Ensure positive prices
                if (open <= 0 || high <= 0 || low <= 0 || close <= 0) {
                    console.error('Non-positive prices in candle:', { time, open, high, low, close });
                    return null;
                }

                return { time, open, high, low, close };
            }).filter(candle => candle !== null).sort((a, b) => a.time - b.time);

            const volumeData = validCandles.map(candle => {
                const volume = parseFloat(candle.volume);
                if (isNaN(volume) || volume <= 0) {
                    console.error('Invalid volume in candle:', candle);
                    return null;
                }
                return {
                    time: candle.timestamp,
                    value: volume,
                    color: candle.close > candle.open ? 
                        this.config.volume.up_color : 
                        this.config.volume.down_color
                };
            }).filter(vol => vol !== null).sort((a, b) => a.time - b.time);
            

            
            chartInstance.candlestickSeries.setData(candleData);

            // Only update volume if chart has volume series
            if (chartInstance.volumeSeries) {
                chartInstance.volumeSeries.setData(volumeData);
            }

            // Store chart data for click handling and lazy loading
            chartInstance.lastChartData = dataset;
            chartInstance.data = candleData;      // lightweight-charts format {time, open, high, low, close}
            chartInstance.volumeData = volumeData; // {time, value, color} — kept in sync by lazy loaders

            // Update chart info
            if (candleData.length > 0 && validCandles.length > 0) {
                // Find the original candle data that corresponds to the last displayed candle
                const lastCandleTime = candleData[candleData.length - 1].time;
                const lastOriginalCandle = validCandles.find(candle => candle.timestamp === lastCandleTime) || validCandles[validCandles.length - 1];
                
                this.updateChartInfo(containerId, lastOriginalCandle, {
                    symbol: dataset.symbol,
                    timeframe: dataset.timeframe,
                    data: dataset.candles
                });
            }


        } catch (error) {
            console.error('Error updating chart data:', error);
            throw error;
        }
    }

    updateChartInfo(containerId, lastCandle, dataset) {
        // lastCandle now contains the original candle data with volume
        const change = ((lastCandle.close - lastCandle.open) / lastCandle.open * 100);
        
        const elements = {
            openPrice: document.getElementById('openPrice'),
            highPrice: document.getElementById('highPrice'),
            lowPrice: document.getElementById('lowPrice'),
            closePrice: document.getElementById('closePrice'),
            volumeInfo: document.getElementById('volumeInfo'),
            changeInfo: document.getElementById('changeInfo'),
            timeInfo: document.getElementById('timeInfo'),
            chartTitle: document.getElementById('chartTitle')
        };
        

        if (elements.openPrice) elements.openPrice.textContent = lastCandle.open.toFixed(4);
        if (elements.highPrice) elements.highPrice.textContent = lastCandle.high.toFixed(4);
        if (elements.lowPrice) elements.lowPrice.textContent = lastCandle.low.toFixed(4);
        
        if (elements.closePrice) {
            elements.closePrice.textContent = lastCandle.close.toFixed(4);
            elements.closePrice.className = `info-value ${change >= 0 ? 'price-up' : 'price-down'}`;
        }
        
        if (elements.changeInfo) {
            elements.changeInfo.textContent = `${change >= 0 ? '+' : ''}${change.toFixed(2)}%`;
            elements.changeInfo.className = `info-value ${change >= 0 ? 'price-up' : 'price-down'}`;
        }

        if (elements.volumeInfo && lastCandle && lastCandle.volume !== undefined && lastCandle.volume !== null) {
            const volume = parseFloat(lastCandle.volume);
            
            if (!isNaN(volume) && volume > 0) {
                const formattedVolume = volume > 1000000 ? 
                    (volume / 1000000).toFixed(2) + 'M' : 
                    volume > 1000 ? (volume / 1000).toFixed(2) + 'K' : 
                    volume.toFixed(0);
                elements.volumeInfo.textContent = formattedVolume;
            } else {
                elements.volumeInfo.textContent = '--';
            }
        } else {
            if (elements.volumeInfo) elements.volumeInfo.textContent = '--';
        }

        // Update time info if available  
        const timeValue = lastCandle.time || lastCandle.timestamp;
        if (elements.timeInfo && timeValue) {
            const date = new Date(timeValue * 1000);
            const year = date.getUTCFullYear();
            const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
            const day = date.getUTCDate().toString().padStart(2, '0');
            const hours = date.getUTCHours().toString().padStart(2, '0');
            const minutes = date.getUTCMinutes().toString().padStart(2, '0');
            elements.timeInfo.textContent = `${year}-${month}-${day} ${hours}:${minutes} UTC`;
        }

        if (elements.chartTitle) {
            const chartInstance = this.charts[containerId];
            const symbol = dataset.symbol || chartInstance.symbol;
            const timeframe = dataset.timeframe || chartInstance.currentTimeframe;
            elements.chartTitle.textContent = `${symbol} • ${timeframe}`;
        }
    }

    async setTimeframe(containerId, timeframe) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        chartInstance.currentTimeframe = timeframe;
        return this.reloadChart(containerId);
    }

    async reloadChart(containerId) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        try {
            // Get current exchange from selects or use defaults
            const exchangeSelect = document.getElementById('exchangeSelect');
            const symbolSelect = document.getElementById('symbolSelect');
            
            const exchange = exchangeSelect ? exchangeSelect.value : '';
            const symbol = symbolSelect ? symbolSelect.value || chartInstance.symbol : chartInstance.symbol;
            
            const data = await this.loadChartData(
                exchange,
                symbol,
                chartInstance.currentTimeframe
            );
            
            this.updateChart(containerId, data);
            return data;
        } catch (error) {
            console.error('Failed to reload chart:', error);
            throw error;
        }
    }

    toggleVolume(containerId) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        chartInstance.volumeVisible = !chartInstance.volumeVisible;
        chartInstance.volumeSeries.applyOptions({
            visible: chartInstance.volumeVisible
        });

        const volumeBtn = document.getElementById('volumeBtn');
        if (volumeBtn) {
            volumeBtn.textContent = chartInstance.volumeVisible ? 'Volume ON' : 'Volume OFF';
            volumeBtn.classList.toggle('active', chartInstance.volumeVisible);
        }
    }

    async updateSymbol(containerId, symbol) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        chartInstance.symbol = symbol;
        return this.reloadChart(containerId);
    }

    setupCrosshairTracking(containerId) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        // Store the last crosshair data for click handling
        let lastCrosshairData = null;
        
        // Simplified crosshair - just store data without updating UI
        chartInstance.chart.subscribeCrosshairMove((param) => {
            const currentInstance = this.charts[containerId];
            if (!currentInstance || currentInstance !== chartInstance) {
                return;
            }
            
            // Only store crosshair data, don't update UI
            if (param.time && param.seriesData && chartInstance.candlestickSeries) {
                try {
                    const data = param.seriesData.get(chartInstance.candlestickSeries);
                    if (data) {
                        const dataWithTime = {
                            ...data,
                            time: param.time
                        };
                        
                        const volumeData = (chartInstance.volumeSeries && param.seriesData.get(chartInstance.volumeSeries)?.value) || 0;
                        
                        // Store for potential click handling
                        lastCrosshairData = {
                            candleData: dataWithTime,
                            volumeData: volumeData
                        };
                        
                        chartInstance.lastCrosshairData = lastCrosshairData;
                    }
                } catch (error) {
                    console.warn('Error in crosshair tracking:', error);
                }
            }
        });
        
        // Add click event listener to show candle details
        chartInstance.chart.subscribeClick((param) => {
            console.log('Chart subscribeClick triggered!', param);
            const currentInstance = this.charts[containerId];
            if (!currentInstance || currentInstance !== chartInstance) {
                return;
            }
            
            // Try to use click data first
            if (param.time && param.seriesData && chartInstance.candlestickSeries) {
                try {
                    const data = param.seriesData.get(chartInstance.candlestickSeries);
                    if (data) {
                        const dataWithTime = {
                            ...data,
                            time: param.time
                        };
                        
                        const volumeValue = (chartInstance.volumeSeries && param.seriesData.get(chartInstance.volumeSeries)?.value) || 0;
                        
                        console.log('Using click data:', dataWithTime);
                        
                        // Update the main info display with clicked candle data
                        const candleWithVolume = {
                            ...dataWithTime,
                            volume: volumeValue || null
                        };
                        
                        this.updateChartInfo(containerId, candleWithVolume, { 
                            symbol: chartInstance.symbol, 
                            timeframe: chartInstance.currentTimeframe,
                            data: []
                        });
                        
                        // Detailed popup removed - info is shown in header
                        return; // Exit early if we have good data
                    }
                } catch (error) {
                    console.warn('Error in click handling:', error);
                }
            }
            
            // Fallback to last crosshair data if click data isn't available
            console.log('Click data not available, trying crosshair data');
            if (lastCrosshairData && lastCrosshairData.candleData) {
                this.showCandleDetails(
                    lastCrosshairData.candleData,
                    lastCrosshairData.volumeData,
                    chartInstance.symbol,
                    chartInstance.currentTimeframe
                );
            } else {
                console.log('No data available for click');
            }
        });
        
        // Alternative click handler on container for real candle data
        const container = document.getElementById(containerId);
        if (container) {
            container.addEventListener('click', (event) => {
                console.log('Container clicked, showing real candle data');
                
                // Try to get candle data from mouse position
                const rect = container.getBoundingClientRect();
                const x = event.clientX - rect.left;
                const y = event.clientY - rect.top;
                
                console.log('Click position:', {x, y, rect});
                
                // Get data at the clicked coordinate
                const timeAtPosition = chartInstance.chart.timeScale().coordinateToTime(x);
                console.log('Time at click position:', timeAtPosition);
                
                if (timeAtPosition) {
                    // Try to find candle data for this time
                    this.findCandleDataByTime(containerId, timeAtPosition);
                } else {
                    // Use the last crosshair data if available (check both local and instance stored)
                    const crosshairData = lastCrosshairData || chartInstance.lastCrosshairData;
                    if (crosshairData && crosshairData.candleData) {
                        console.log('Using crosshair data for container click:', crosshairData);
                        
                        // Update the main info display
                        // Create candle object with volume for updateChartInfo
                        const candleWithVolume = {
                            ...crosshairData.candleData,
                            volume: crosshairData.volumeData ? crosshairData.volumeData.value : null
                        };
                        
                        this.updateChartInfo(containerId, candleWithVolume, { 
                            symbol: chartInstance.symbol, 
                            timeframe: chartInstance.currentTimeframe,
                            data: []
                        });
                        
                        // Detailed popup removed - info is shown in header
                    }
                }
            });
        }
    }
    
    showSimpleClickTest(containerId) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) return;
        
        // Create a simple test popup
        let testDiv = document.getElementById('clickTest');
        if (testDiv) testDiv.remove();
        
        testDiv = document.createElement('div');
        testDiv.id = 'clickTest';
        testDiv.style.cssText = `
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: #1e222d;
            border: 2px solid #2962ff;
            border-radius: 8px;
            padding: 20px;
            color: #d1d4dc;
            font-size: 14px;
            z-index: 1000;
            text-align: center;
        `;
        testDiv.innerHTML = `
            <div>🎯 Chart Click Test</div>
            <div>Symbol: ${chartInstance.symbol}</div>
            <div>Timeframe: ${chartInstance.currentTimeframe}</div>
            <button onclick="this.parentElement.remove()" style="margin-top: 10px; background: #2962ff; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer;">Close</button>
        `;
        document.body.appendChild(testDiv);
        
        // Auto close after 3 seconds
        setTimeout(() => {
            if (testDiv && testDiv.parentElement) {
                testDiv.remove();
            }
        }, 3000);
    }
    
    showHoverInstructionPopup() {
        // Show instruction popup
        let instructionDiv = document.getElementById('hoverInstruction');
        if (instructionDiv) instructionDiv.remove();
        
        instructionDiv = document.createElement('div');
        instructionDiv.id = 'hoverInstruction';
        instructionDiv.style.cssText = `
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: #1e222d;
            border: 2px solid #f39c12;
            border-radius: 8px;
            padding: 20px;
            color: #d1d4dc;
            font-size: 14px;
            z-index: 1000;
            text-align: center;
            max-width: 300px;
        `;
        instructionDiv.innerHTML = `
            <div style="font-size: 18px; margin-bottom: 10px;">💡</div>
            <div style="margin-bottom: 15px;"><strong>Як переглянути дані свічі:</strong></div>
            <div style="margin-bottom: 10px;">1. Наведіть курсор на свічу</div>
            <div style="margin-bottom: 10px;">2. Потім клікніть</div>
            <div style="font-size: 12px; color: #868b94; margin-top: 15px;">
                Або просто рухайте мишкою по графіку щоб переглядати дані у верхній частині
            </div>
            <button onclick="this.parentElement.remove()" style="margin-top: 15px; background: #f39c12; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;">Зрозуміло</button>
        `;
        document.body.appendChild(instructionDiv);
        
        // Auto close after 8 seconds
        setTimeout(() => {
            if (instructionDiv && instructionDiv.parentElement) {
                instructionDiv.remove();
            }
        }, 8000);
    }
    
    findCandleDataByTime(containerId, timeAtPosition) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance || !chartInstance.lastChartData) {
            console.log('No chart data available for time lookup');
            this.showHoverInstructionPopup();
            return;
        }
        
        console.log('Looking for candle data at time:', timeAtPosition);
        console.log('Available chart data:', chartInstance.lastChartData);
        
        // Find the closest candle to the clicked time
        let closestCandle = null;
        let closestDistance = Infinity;
        
        if (chartInstance.lastChartData && chartInstance.lastChartData.candles) {
            for (const candle of chartInstance.lastChartData.candles) {
                const distance = Math.abs(candle.timestamp - timeAtPosition);
                if (distance < closestDistance) {
                    closestDistance = distance;
                    closestCandle = candle;
                }
            }
        }
        
        if (closestCandle) {
            console.log('Found closest candle:', closestCandle);
            
            // Convert to the format expected by showCandleDetails
            const candleData = {
                time: this.getCandleCloseTime(closestCandle.timestamp, chartInstance.currentTimeframe),
                open: parseFloat(closestCandle.open),
                high: parseFloat(closestCandle.high),
                low: parseFloat(closestCandle.low),
                close: parseFloat(closestCandle.close),
                volume: parseFloat(closestCandle.volume) || 0
            };
            
            // Update the main info display
            this.updateChartInfo(containerId, candleData, { 
                symbol: chartInstance.symbol, 
                timeframe: chartInstance.currentTimeframe,
                data: []
            });
            
            // Detailed popup removed - info is shown in header
        } else {
            console.log('No candle found for time:', timeAtPosition);
        }
    }

    setupAutoResize(containerId) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        const resizeHandler = () => {
            const container = document.getElementById(containerId);
            if (container && chartInstance.chart) {
                // Force full width calculation
                const fullWidth = container.clientWidth || container.offsetWidth || window.innerWidth - 40;
                chartInstance.chart.applyOptions({
                    width: fullWidth,
                });
                console.log(`Chart resized to width: ${fullWidth}px`);
            }
        };

        window.addEventListener('resize', resizeHandler);
        chartInstance.resizeHandler = resizeHandler;
    }

    // Indicator methods
    async loadIndicatorData(indicatorName, exchange, symbol, timeframe, options = {}) {
        // Try different indicator name variations in order of likelihood
        const indicatorNamesToTry = options.indicatorVariations || [
            indicatorName,  // Original: sol_rsi_14_1h
            indicatorName.replace(/_\d+[hmdsw]$/i, ''), // Remove timeframe: sol_rsi_14
            indicatorName.split('_').slice(0, -1).join('_'), // Remove last part: sol_rsi_14
            `${symbol.toLowerCase()}_rsi_14_1h`, // Try with symbol prefix
            `${symbol.toLowerCase()}_rsi`, // Symbol + indicator type
            'rsi', // Simple name
        ];
        
        // Try different timeframes that might be stored in database
        const timeframesToTry = options.timeframeVariations || [timeframe, '4h', '1h', '1d', '15m', '5m'];
        
        // Allow specifying candle source preference for indicators
        const preferAggregatedCandles = options.prefer_aggregated_candles !== undefined ? options.prefer_aggregated_candles : true;
        
        for (const indicatorToTry of indicatorNamesToTry) {
            for (const tf of timeframesToTry) {
                const url = `${this.config.api.base_url}/indicators?indicator_name=${encodeURIComponent(indicatorToTry)}&exchange=${encodeURIComponent(exchange)}&symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(tf)}&limit=500&prefer_aggregated_candles=${preferAggregatedCandles}`;
                
                console.log(`🔍 Trying: ${indicatorToTry} for ${exchange}/${symbol}/${tf}`);
                
                try {
                    const response = await fetch(url);
                    
                    if (!response.ok) {
                        console.log(`❌ HTTP ${response.status} for ${indicatorToTry}/${tf}`);
                        continue;
                    }
                    
                    const data = await response.json();
                    
                    if (data.error) {
                        console.log(`⚠️ API Error for ${indicatorToTry}/${tf}: ${data.error}`);
                        
                        // If we get available indicators list, log it for debugging
                        if (data.available_indicators) {
                            console.log('Available indicators:', data.available_indicators);
                        }
                        continue;
                    }
                    
                    if (data.data && data.data.indicators && data.data.indicators.length > 0) {
                        console.log(`✅ SUCCESS: Found ${data.data.indicators.length} data points for ${indicatorToTry}`);
                        console.log(`📊 First data point:`, data.data.indicators[0]);
                        console.log(`📊 Last data point:`, data.data.indicators[data.data.indicators.length - 1]);
                        return {
                            data: data.data.indicators,
                            count: data.data.count,
                            indicator_name: data.data.indicator_name,
                            exchange: data.data.exchange,
                            symbol: data.data.symbol,
                            timeframe: data.data.timeframe
                        };
                    } else {
                        console.log(`⚠️ Empty data for ${indicatorToTry}/${tf}`);
                        continue;
                    }
                } catch (error) {
                    console.log(`❌ Network error for ${indicatorToTry}/${tf}:`, error.message);
                    continue;
                }
            }
        }
        
        // If all attempts failed, provide detailed error
        const allUrls = indicatorNamesToTry.flatMap(ind => 
            timeframesToTry.map(tf => 
                `${this.config.api.base_url}/indicators/${encodeURIComponent(ind)}/data?exchange=${exchange}&symbol=${symbol}&timeframe=${tf}`
            )
        );
        
        console.error(`Failed to load indicator data. Tried ${allUrls.length} combinations:`, allUrls);
        throw new Error(`No data available for any variation of ${indicatorName}. Tried ${indicatorNamesToTry.length} name variations and ${timeframesToTry.length} timeframes.`);
    }

    addIndicatorToChart(containerId, indicatorName, indicatorData, style = {}) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        if (chartInstance.indicators[indicatorName]) {
            chartInstance.chart.removeSeries(chartInstance.indicators[indicatorName]);
        }

        // Determine indicator type and set appropriate scale
        const isOscillator = indicatorName.toLowerCase().includes('rsi') || 
                           indicatorName.toLowerCase().includes('stoch') ||
                           indicatorName.toLowerCase().includes('williams') ||
                           indicatorName.toLowerCase().includes('cci');

        let seriesOptions = {
            color: style.color || '#ff6b6b',
            lineWidth: style.lineWidth || 2,
            title: indicatorName.toUpperCase(),
            ...style
        };

        // For oscillators like RSI, create separate price scale
        if (isOscillator) {
            seriesOptions.priceScaleId = `${indicatorName}_scale`;
            seriesOptions.scaleMargins = {
                top: 0.1,
                bottom: 0.1,
            };
        }

        const indicatorSeries = chartInstance.chart.addLineSeries(seriesOptions);

        // Configure price scale for oscillators
        if (isOscillator) {
            chartInstance.chart.priceScale(`${indicatorName}_scale`).applyOptions({
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.1,
                },
                // For RSI, set fixed range 0-100
                ...(indicatorName.toLowerCase().includes('rsi') && {
                    autoScale: false,
                    mode: 1, // Normal mode
                })
            });
        }

        if (indicatorData.data && indicatorData.data.length > 0) {
            const formattedData = indicatorData.data.map(point => ({
                time: point.timestamp,
                value: point.value
            }));

            console.log(`Adding ${formattedData.length} data points for ${indicatorName}:`, formattedData.slice(0, 3));
            indicatorSeries.setData(formattedData);
        }

        chartInstance.indicators[indicatorName] = indicatorSeries;
        return indicatorSeries;
    }

    removeIndicatorFromChart(containerId, indicatorName) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance || !chartInstance.indicators[indicatorName]) {
            return;
        }

        chartInstance.chart.removeSeries(chartInstance.indicators[indicatorName]);
        delete chartInstance.indicators[indicatorName];
    }

    async loadAndDisplayIndicator(containerId, indicatorName, style = {}, options = {}) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        try {
            const exchangeSelect = document.getElementById('exchangeSelect');
            const symbolSelect = document.getElementById('symbolSelect');
            
            const exchange = exchangeSelect ? exchangeSelect.value : '';
            const symbol = symbolSelect ? symbolSelect.value || chartInstance.symbol : chartInstance.symbol;
            
            const indicatorData = await this.loadIndicatorData(
                indicatorName,
                exchange,
                symbol,
                chartInstance.currentTimeframe,
                options
            );

            this.addIndicatorToChart(containerId, indicatorName, indicatorData, style);
            return indicatorData;
        } catch (error) {
            console.error(`Failed to load indicator ${indicatorName}:`, error);
            throw error;
        }
    }

    getAvailableIndicators() {
        return fetch(`${this.config.api.base_url}/indicators/status`)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    throw new Error(data.error);
                }
                if (data.indicators && Array.isArray(data.indicators)) {
                    return data.indicators.map(indicator => indicator.name);
                }
                return [];
            })
            .catch(error => {
                console.error('Failed to get available indicators:', error);
                return [];
            });
    }

    destroy(containerId) {
        const chartInstance = this.charts[containerId];
        if (chartInstance) {
            if (chartInstance.resizeHandler) {
                window.removeEventListener('resize', chartInstance.resizeHandler);
            }
            
            if (chartInstance.chart) {
                chartInstance.chart.remove();
            }
            
            delete this.charts[containerId];
        }
    }

    destroyAll() {
        Object.keys(this.charts).forEach(containerId => {
            this.destroy(containerId);
        });
    }

    // Create price chart without volume (for indicators page)
    createPriceOnlyChart(containerId, symbol, timeframe) {
        console.log(`Creating price-only chart for container: ${containerId}`);
        
        // Destroy existing chart if it exists to prevent race conditions
        if (this.charts[containerId]) {
            console.log(`Destroying existing chart for ${containerId}`);
            this.destroy(containerId);
        }
        
        const container = document.getElementById(containerId);
        if (!container) {
            throw new Error(`Container ${containerId} not found`);
        }

        if (typeof LightweightCharts === 'undefined') {
            throw new Error('LightweightCharts library is not loaded');
        }

        // Clear container
        container.innerHTML = '';

        try {
            // Force proper width calculation - use full available width
            const containerWidth = container.clientWidth || container.offsetWidth || window.innerWidth - 40;
            const actualWidth = containerWidth > 0 ? containerWidth : window.innerWidth - 40;
            
            const chart = LightweightCharts.createChart(container, {
                width: actualWidth,
                height: this.config.chart.height || 500,
                layout: {
                    backgroundColor: this.config.chart.background_color,
                    textColor: this.config.chart.text_color,
                },
                grid: {
                    vertLines: { color: this.config.chart.grid_color },
                    horzLines: { color: this.config.chart.grid_color },
                },
                crosshair: {
                    mode: LightweightCharts.CrosshairMode.Normal,
                    vertLine: {
                        width: 1,
                        color: this.config.chart.crosshair_color || '#758696',
                        style: LightweightCharts.LineStyle.Solid,
                        visible: true,
                        labelVisible: true,
                    },
                    horzLine: {
                        width: 1,
                        color: this.config.chart.crosshair_color || '#758696',
                        style: LightweightCharts.LineStyle.Solid,
                        visible: true,
                        labelVisible: true,
                    },
                },
                rightPriceScale: {
                    borderColor: this.config.chart.border_color,
                    visible: true,
                    borderVisible: true,
                },
                timeScale: {
                    borderColor: this.config.chart.border_color,
                    timeVisible: true,
                    secondsVisible: false,
                    shiftVisibleRangeOnNewBar: true,
                    fixLeftEdge: true,
                    fixRightEdge: true,
                    lockVisibleTimeRangeOnResize: true,
                    rightBarStaysOnScroll: true,
                    borderVisible: true,
                    visible: true,
                },
                localization: {
                    timeFormatter: (time) => {
                        try {
                            const date = new Date(time * 1000);
                            if (isNaN(date.getTime())) {
                                return 'Invalid Date';
                            }
                            const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
                            const day = date.getUTCDate().toString().padStart(2, '0');
                            const hours = date.getUTCHours().toString().padStart(2, '0');
                            const minutes = date.getUTCMinutes().toString().padStart(2, '0');
                            const seconds = date.getUTCSeconds().toString().padStart(2, '0');
                            return `${month}-${day} ${hours}:${minutes}:${seconds}`;
                        } catch (error) {
                            console.error('Time formatting error:', error);
                            return 'Time Error';
                        }
                    },
                },
            });

            const candlestickSeries = chart.addCandlestickSeries({
                upColor: this.config.candlestick.up_color,
                downColor: this.config.candlestick.down_color,
                borderVisible: false,
                wickUpColor: this.config.candlestick.up_color,
                wickDownColor: this.config.candlestick.down_color,
            });

            // Keep candlestick in top 80% so volume has room at the bottom
            chart.priceScale('right').applyOptions({
                scaleMargins: { top: 0.0, bottom: 0.2 },
            });

            const volumeSeries = chart.addHistogramSeries({
                color: '#26a69a',
                priceFormat: { type: 'volume' },
                priceScaleId: 'volume',
            });

            chart.priceScale('volume').applyOptions({
                scaleMargins: {
                    top: this.config.volume.scale_top,
                    bottom: this.config.volume.scale_bottom,
                },
            });

            this.charts[containerId] = {
                chart,
                candlestickSeries,
                volumeSeries,
                symbol: symbol || 'UNKNOWN',
                currentTimeframe: timeframe,
                volumeVisible: true,
                indicators: {},
                type: 'price_only'
            };

            console.log(`Price-only chart created successfully for ${containerId}`);
            return this.charts[containerId];

        } catch (error) {
            console.error('Error creating price-only chart:', error);
            throw error;
        }
    }

    // Create indicator-only chart
    createIndicatorChart(containerId, indicatorName, timeframe) {
        console.log(`Creating indicator chart for container: ${containerId}`);
        
        const container = document.getElementById(containerId);
        if (!container) {
            throw new Error(`Container ${containerId} not found`);
        }

        if (typeof LightweightCharts === 'undefined') {
            throw new Error('LightweightCharts library is not loaded');
        }

        // Clear container
        container.innerHTML = '';

        try {
            // Force proper width calculation for indicator chart
            const containerWidth = container.clientWidth || container.parentElement?.clientWidth || container.offsetWidth || 800;
            const actualWidth = Math.max(containerWidth, 800);
            
            const chart = LightweightCharts.createChart(container, {
                width: actualWidth,
                height: 300,
                layout: {
                    backgroundColor: this.config.chart.background_color,
                    textColor: this.config.chart.text_color,
                },
                grid: {
                    vertLines: { color: this.config.chart.grid_color },
                    horzLines: { color: this.config.chart.grid_color },
                },
                crosshair: {
                    mode: LightweightCharts.CrosshairMode.Normal,
                    vertLine: {
                        width: 1,
                        color: this.config.chart.crosshair_color || '#758696',
                        style: LightweightCharts.LineStyle.Solid,
                        visible: true,
                        labelVisible: true,
                    },
                    horzLine: {
                        width: 1,
                        color: this.config.chart.crosshair_color || '#758696',
                        style: LightweightCharts.LineStyle.Solid,
                        visible: true,
                        labelVisible: true,
                    },
                },
                rightPriceScale: {
                    borderColor: this.config.chart.border_color,
                    visible: true,
                    borderVisible: true,
                },
                timeScale: {
                    borderColor: this.config.chart.border_color,
                    timeVisible: true,
                    secondsVisible: false,
                    fixLeftEdge: false,
                    fixRightEdge: false,
                    rightBarStaysOnScroll: true,
                    borderVisible: true,
                    visible: true,
                },
                localization: {
                    timeFormatter: (time) => {
                        try {
                            const date = new Date(time * 1000);
                            if (isNaN(date.getTime())) return 'Invalid Date';
                            const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
                            const day = date.getUTCDate().toString().padStart(2, '0');
                            const hours = date.getUTCHours().toString().padStart(2, '0');
                            const minutes = date.getUTCMinutes().toString().padStart(2, '0');
                            return `${month}-${day} ${hours}:${minutes}`;
                        } catch (error) {
                            return 'Time Error';
                        }
                    },
                },
            });

            this.charts[containerId] = {
                chart,
                currentTimeframe: timeframe,
                indicatorName: indicatorName,
                type: 'indicator'
            };

            console.log(`Indicator chart created successfully for ${containerId}`);
            return this.charts[containerId];

        } catch (error) {
            console.error('Error creating indicator chart:', error);
            throw error;
        }
    }

    // Display indicator data on indicator chart
    displayIndicatorData(containerId, indicatorData, indicatorName) {
        const chartInstance = this.charts[containerId];
        if (!chartInstance) {
            throw new Error(`Chart ${containerId} not found`);
        }

        // Remove existing series if any
        if (chartInstance.indicatorSeries) {
            chartInstance.chart.removeSeries(chartInstance.indicatorSeries);
        }

        // Create line series for indicator
        const indicatorSeries = chartInstance.chart.addLineSeries({
            color: '#ff6b6b',
            lineWidth: 2,
            title: indicatorName.toUpperCase()
        });

        if (indicatorData.data && indicatorData.data.length > 0) {
            const maxBars = {
                '1m': 1440, '5m': 1440, '15m': 672, '30m': 336,
                '1h': 720,  '4h': 360,  '1d': 365, '1w': 156,
            };
            const limit = maxBars[chartInstance.currentTimeframe] || 500;

            const formattedData = indicatorData.data
                .filter(point => {
                    if (!point || point.timestamp == null || point.value == null) return false;
                    return Number.isFinite(parseFloat(point.timestamp)) &&
                           Number.isFinite(parseFloat(point.value));
                })
                .map(point => ({
                    time: parseFloat(point.timestamp),
                    value: parseFloat(point.value)
                }))
                .sort((a, b) => a.time - b.time)
                .slice(-limit);

            if (formattedData.length > 0) {
                indicatorSeries.setData(formattedData);
                chartInstance.chart.timeScale().fitContent();
            } else {
                console.warn('No valid indicator data points after filtering');
            }

            // Update indicator info
            this.updateIndicatorInfo(indicatorData, indicatorName);
        }

        chartInstance.indicatorSeries = indicatorSeries;
        return indicatorSeries;
    }

    // Update indicator statistics
    updateIndicatorInfo(indicatorData, indicatorName) {
        if (!indicatorData.data || indicatorData.data.length === 0) return;

        const values = indicatorData.data.map(d => d.value);
        const current = values[values.length - 1];
        const min = Math.min(...values);
        const max = Math.max(...values);
        const avg = values.reduce((a, b) => a + b, 0) / values.length;

        const elements = {
            title: document.getElementById('indicatorChartTitle'),
            current: document.getElementById('indicatorCurrentValue'),
            min: document.getElementById('indicatorMinValue'),
            max: document.getElementById('indicatorMaxValue'),
            avg: document.getElementById('indicatorAvgValue')
        };

        if (elements.title) elements.title.textContent = `${indicatorName.toUpperCase()} Chart`;
        if (elements.current) elements.current.textContent = current.toFixed(2);
        if (elements.min) elements.min.textContent = min.toFixed(2);
        if (elements.max) elements.max.textContent = max.toFixed(2);
        if (elements.avg) elements.avg.textContent = avg.toFixed(2);
    }
    
    showCandleDetails(candleData, volume, symbol, timeframe) {
        // Create or update a popup/modal with detailed candle information
        let detailsDiv = document.getElementById('candleDetails');
        
        if (!detailsDiv) {
            // Create details div if it doesn't exist
            detailsDiv = document.createElement('div');
            detailsDiv.id = 'candleDetails';
            detailsDiv.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                background: #1e222d;
                border: 1px solid #2a2e39;
                border-radius: 8px;
                padding: 15px;
                color: #d1d4dc;
                font-family: monospace;
                font-size: 12px;
                z-index: 1000;
                min-width: 250px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            `;
            document.body.appendChild(detailsDiv);
            
            // Add close button
            const closeBtn = document.createElement('button');
            closeBtn.innerHTML = '×';
            closeBtn.style.cssText = `
                position: absolute;
                top: 5px;
                right: 8px;
                background: none;
                border: none;
                color: #868b94;
                font-size: 16px;
                cursor: pointer;
                padding: 0;
                width: 20px;
                height: 20px;
            `;
            closeBtn.onclick = () => detailsDiv.remove();
            detailsDiv.appendChild(closeBtn);
        }
        
        // Format the timestamp
        const date = new Date(candleData.time * 1000);
        const formattedTime = date.toUTCString();
        
        // Calculate price change
        const change = candleData.close - candleData.open;
        const changePercent = ((change / candleData.open) * 100);
        const changeColor = change >= 0 ? '#26a69a' : '#ef5350';
        
        // Calculate candle body and wick sizes
        const bodySize = Math.abs(candleData.close - candleData.open);
        const upperWick = candleData.high - Math.max(candleData.open, candleData.close);
        const lowerWick = Math.min(candleData.open, candleData.close) - candleData.low;
        const totalRange = candleData.high - candleData.low;
        
        // Format volume
        const formattedVolume = volume > 1000000 ? 
            (volume / 1000000).toFixed(2) + 'M' : 
            volume > 1000 ? (volume / 1000).toFixed(2) + 'K' : 
            volume.toFixed(0);
        
        detailsDiv.innerHTML = `
            <button onclick="this.parentElement.remove()" style="position: absolute; top: 5px; right: 8px; background: none; border: none; color: #868b94; font-size: 16px; cursor: pointer;">×</button>
            <div style="font-size: 14px; font-weight: bold; margin-bottom: 10px; color: #d1d4dc;">
                📊 ${symbol} • ${timeframe.toUpperCase()}
            </div>
            <div style="font-size: 11px; color: #868b94; margin-bottom: 8px;">
                ${formattedTime}
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 11px;">
                <div>📈 <strong>Open:</strong> ${candleData.open.toFixed(4)}</div>
                <div>🔥 <strong>High:</strong> ${candleData.high.toFixed(4)}</div>
                <div>❄️ <strong>Low:</strong> ${candleData.low.toFixed(4)}</div>
                <div>🎯 <strong>Close:</strong> ${candleData.close.toFixed(4)}</div>
            </div>
            <div style="margin: 8px 0; padding: 8px; background: rgba(42, 46, 57, 0.5); border-radius: 4px;">
                <div style="color: ${changeColor}; font-weight: bold;">
                    📊 Change: ${change >= 0 ? '+' : ''}${change.toFixed(4)} (${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(2)}%)
                </div>
            </div>
            <div style="font-size: 11px; color: #868b94;">
                <div>📈 Volume: <strong style="color: #d1d4dc;">${formattedVolume}</strong></div>
                <div>📏 Range: <strong>${totalRange.toFixed(4)}</strong></div>
                <div>🟫 Body: <strong>${bodySize.toFixed(4)}</strong> (${((bodySize/totalRange)*100).toFixed(1)}%)</div>
                <div>⬆️ Upper Wick: <strong>${upperWick.toFixed(4)}</strong></div>
                <div>⬇️ Lower Wick: <strong>${lowerWick.toFixed(4)}</strong></div>
            </div>
            <div style="margin-top: 8px; font-size: 10px; color: #666;">
                💡 Click anywhere to close this popup
            </div>
        `;
        
        // Auto-close after 10 seconds
        setTimeout(() => {
            if (detailsDiv && detailsDiv.parentElement) {
                detailsDiv.remove();
            }
        }, 10000);
        
        // Click anywhere to close
        const closeOnClick = (e) => {
            if (detailsDiv && detailsDiv.parentElement && !detailsDiv.contains(e.target)) {
                detailsDiv.remove();
                document.removeEventListener('click', closeOnClick);
            }
        };
        setTimeout(() => document.addEventListener('click', closeOnClick), 100);
        
        console.log('Candle details displayed:', candleData);
    }
}

// Export for use
window.ChartManager = ChartManager;
console.log('ChartManager class defined and exported to window');