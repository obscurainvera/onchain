import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Spinner, Alert, Badge } from 'react-bootstrap';
import './TokenAddPage.css';
import { API_BASE_URL } from '../services/api';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TokenAddPage = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [tokenAddress, setTokenAddress] = useState('');
  const [pairAddress, setPairAddress] = useState('');
  const [tokenInfo, setTokenInfo] = useState(null);
  const [pairAge, setPairAge] = useState(null);
  const [isOldToken, setIsOldToken] = useState(false);
  const [showEMAFields, setShowEMAFields] = useState(false);
  const [addedBy, setAddedBy] = useState('obscurainvera');
  const [timeframes, setTimeframes] = useState(['30min', '1h', '4h']);

  // Get the most recent COMPLETED interval time based on current UTC time
  const getMostRecentIntervalTime = (interval) => {
    const now = new Date();
    const utcHour = now.getUTCHours();
    const utcMinute = now.getUTCMinutes();
    
    let targetHour, targetMinute;
    
    if (interval === '15m') {
      // For 15m: Get the previous completed 15-minute candle
      // Current 15m interval started at Math.floor(utcMinute / 15) * 15
      // Previous completed candle is one interval back
      const currentIntervalStart = Math.floor(utcMinute / 15) * 15;
      if (currentIntervalStart === 0) {
        // If we're in the first 15 minutes of the hour, go back to previous hour
        targetHour = utcHour === 0 ? 23 : utcHour - 1;
        targetMinute = 45;
      } else {
        targetHour = utcHour;
        targetMinute = currentIntervalStart - 15;
      }
    } else if (interval === '30min') {
      // For 30min: Get the previous completed 30-minute candle
      // Current 30m interval started at Math.floor(utcMinute / 30) * 30
      // Previous completed candle is one interval back
      const currentIntervalStart = Math.floor(utcMinute / 30) * 30;
      if (currentIntervalStart === 0) {
        // If we're in the first 30 minutes of the hour, go back to previous hour
        targetHour = utcHour === 0 ? 23 : utcHour - 1;
        targetMinute = 30;
      } else {
        targetHour = utcHour;
        targetMinute = currentIntervalStart - 30;
      }
    } else if (interval === '1h') {
      // For 1h: Get the previous completed 1-hour candle
      // We need a fully completed hour, so go back one hour
      if (utcHour === 0) {
        targetHour = 23;
      } else {
        targetHour = utcHour - 1;
      }
      targetMinute = 0;
    } else if (interval === '4h') {
      // For 4h: Get the previous completed 4-hour candle
      // 4h intervals: 0-4, 4-8, 8-12, 12-16, 16-20, 20-24
      const hours4h = [0, 4, 8, 12, 16, 20];
      const currentInterval = hours4h.find((h, i) => 
        utcHour >= h && (i === hours4h.length - 1 || utcHour < hours4h[i + 1])
      );
      
      // Get the previous completed 4h interval
      const currentIndex = hours4h.indexOf(currentInterval);
      if (currentIndex === 0) {
        // If we're in the first 4h interval (0-4), go back to previous day's last interval
        targetHour = 20;
      } else {
        targetHour = hours4h[currentIndex - 1];
      }
      targetMinute = 0;
    }
    
    // Convert to 12-hour format
    const hour12 = targetHour === 0 ? 12 : targetHour > 12 ? targetHour - 12 : targetHour;
    const ampm = targetHour < 12 ? 'AM' : 'PM';
    return `${hour12.toString().padStart(2, '0')}:${targetMinute.toString().padStart(2, '0')} ${ampm}`;
  };
  
  // EMA data state - Initialize with current UTC time-based defaults
  const [emaData, setEmaData] = useState(() => {
    return {
      ema21: {
        '30min': { value: '', referenceTime: getMostRecentIntervalTime('30min') },
        '1h': { value: '', referenceTime: getMostRecentIntervalTime('1h') },
        '4h': { value: '', referenceTime: getMostRecentIntervalTime('4h') }
      },
      ema34: {
        '30min': { value: '', referenceTime: getMostRecentIntervalTime('30min') },
        '1h': { value: '', referenceTime: getMostRecentIntervalTime('1h') },
        '4h': { value: '', referenceTime: getMostRecentIntervalTime('4h') }
      }
    };
  });

  // AVWAP data state - Initialize with current UTC time-based defaults
  const [avwapData, setAvwapData] = useState(() => {
    return {
      '30min': { value: '', referenceTime: getMostRecentIntervalTime('30min') },
      '1h': { value: '', referenceTime: getMostRecentIntervalTime('1h') },
      '4h': { value: '', referenceTime: getMostRecentIntervalTime('4h') }
    };
  });

  // Generate time options for different timeframes
  const generateTimeOptions = (interval) => {
    const times = [];
    
    if (interval === '15m') {
      // 15-minute intervals: 00:00, 00:15, 00:30, 00:45, etc.
      for (let hour = 0; hour < 24; hour++) {
        for (let minute = 0; minute < 60; minute += 15) {
          const hour12 = hour === 0 ? 12 : hour > 12 ? hour - 12 : hour;
          const ampm = hour < 12 ? 'AM' : 'PM';
          const timeString = `${hour12.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')} ${ampm}`;
          times.push(timeString);
        }
      }
    } else if (interval === '30min') {
      // 30-minute intervals: 00:00, 00:30, 01:00, 01:30, etc.
      for (let hour = 0; hour < 24; hour++) {
        for (let minute = 0; minute < 60; minute += 30) {
          const hour12 = hour === 0 ? 12 : hour > 12 ? hour - 12 : hour;
          const ampm = hour < 12 ? 'AM' : 'PM';
          const timeString = `${hour12.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')} ${ampm}`;
          times.push(timeString);
        }
      }
    } else if (interval === '1h') {
      // 1-hour intervals: 01:00, 02:00, 03:00, etc.
      for (let hour = 0; hour < 24; hour++) {
        const hour12 = hour === 0 ? 12 : hour > 12 ? hour - 12 : hour;
        const ampm = hour < 12 ? 'AM' : 'PM';
        const timeString = `${hour12.toString().padStart(2, '0')}:00 ${ampm}`;
        times.push(timeString);
      }
    } else if (interval === '4h') {
      // 4-hour intervals: 12:00 AM, 04:00 AM, 08:00 AM, 12:00 PM, 04:00 PM, 08:00 PM
      const hours4h = [0, 4, 8, 12, 16, 20];
      for (const hour of hours4h) {
        const hour12 = hour === 0 ? 12 : hour > 12 ? hour - 12 : hour;
        const ampm = hour < 12 ? 'AM' : 'PM';
        const timeString = `${hour12.toString().padStart(2, '0')}:00 ${ampm}`;
        times.push(timeString);
      }
    }
    
    return times;
  };

  // Check if token address is valid and fetch token info
  const handleTokenAddressChange = async (address) => {
    setTokenAddress(address);
    setTokenInfo(null);
    setPairAge(null);
    setIsOldToken(false);
    setShowEMAFields(false);
    setError(null);

    if (!address || address.length < 32) return;

    try {
      setLoading(true);
      
      // Make a request to DexScreener to get token info
      const response = await axios.get(`${API_BASE_URL}/api/price/token/${address}`);
      
      if (response.data && response.data.status === 'success' && response.data.data) {
        const tokenData = response.data.data;
        setTokenInfo(tokenData);
        
        // Calculate pair age using pairCreatedAt from API
        const currentTime = Date.now();
        const pairCreatedTime = tokenData.pairCreatedAt; // Already in milliseconds
        const ageInDays = (currentTime - pairCreatedTime) / (1000 * 60 * 60 * 24);
        setPairAge(ageInDays);
        
        const isOld = ageInDays > 7;
        setIsOldToken(isOld);
        setShowEMAFields(isOld);
        
        // Auto-fill pair address from API response
        if (tokenData.pairAddress) {
          setPairAddress(tokenData.pairAddress);
        }
        
        if (isDev) {
          console.log('Token info:', tokenData);
          console.log('Pair age:', ageInDays, 'days');
        }
      }
    } catch (err) {
      if (isDev) console.error('Error fetching token info:', err);
      // Don't show error for partial addresses
      if (address.length >= 32) {
        setError('Unable to fetch token information. Please check the token address.');
      }
    } finally {
      setLoading(false);
    }
  };

  // Handle EMA data changes
  const handleEMAChange = (emaType, timeframe, field, value) => {
    setEmaData(prev => ({
      ...prev,
      [emaType]: {
        ...prev[emaType],
        [timeframe]: {
          ...prev[emaType][timeframe],
          [field]: value
        }
      }
    }));
  };

  // Handle AVWAP data changes
  const handleAVWAPChange = (timeframe, field, value) => {
    setAvwapData(prev => ({
      ...prev,
      [timeframe]: {
        ...prev[timeframe],
        [field]: value
      }
    }));
  };

  // Validate form data
  const validateForm = () => {
    if (!tokenAddress.trim()) {
      setError('Token address is required');
      return false;
    }

    if (!pairAddress.trim()) {
      setError('Pair address is required');
      return false;
    }

    if (!addedBy.trim()) {
      setError('Added by field is required');
      return false;
    }

    if (!timeframes || timeframes.length === 0) {
      setError('At least one timeframe is required');
      return false;
    }

    // Validate AVWAP data (required for all tokens)
    for (const timeframe of ['30min', '1h', '4h']) {
      if (!avwapData[timeframe].value) {
        setError(`AVWAP ${timeframe.toUpperCase()} value is required for all tokens`);
        return false;
      }
    }

    if (isOldToken) {
      // Validate EMA data
      for (const emaType of ['ema21', 'ema34']) {
        for (const timeframe of ['30min', '1h', '4h']) {
          if (!emaData[emaType][timeframe].value) {
            setError(`${emaType.toUpperCase()} ${timeframe.toUpperCase()} value is required for old tokens`);
            return false;
          }
        }
      }
    }

    return true;
  };

  // Submit form
  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    setSuccess(null);

    if (!validateForm()) return;

    setLoading(true);

    try {
      const requestData = {
        tokenAddress: tokenAddress.trim(),
        pairAddress: pairAddress.trim(),
        addedBy: addedBy.trim(),
        timeframes: timeframes,
        avwap: avwapData  // AVWAP data is required for all tokens
      };

      // Add EMA data for old tokens
      if (isOldToken) {
        requestData.ema21 = emaData.ema21;
        requestData.ema34 = emaData.ema34;
      }

      const response = await axios.post(`${API_BASE_URL}/api/tokens/add`, requestData);

      if (response.data.success) {
        setSuccess(`Token ${tokenInfo?.symbol || 'successfully'} added to tracking!`);
        // Reset form
        setTokenAddress('');
        setPairAddress('');
        setAddedBy('obscurainvera');
        setTimeframes(['30min', '1h', '4h']);
        setTokenInfo(null);
        setPairAge(null);
        setIsOldToken(false);
        setShowEMAFields(false);
        setEmaData({
          ema21: {
            '30min': { value: '', referenceTime: getMostRecentIntervalTime('30min') },
            '1h': { value: '', referenceTime: getMostRecentIntervalTime('1h') },
            '4h': { value: '', referenceTime: getMostRecentIntervalTime('4h') }
          },
          ema34: {
            '30min': { value: '', referenceTime: getMostRecentIntervalTime('30min') },
            '1h': { value: '', referenceTime: getMostRecentIntervalTime('1h') },
            '4h': { value: '', referenceTime: getMostRecentIntervalTime('4h') }
          }
        });
        setAvwapData({
          '30min': { value: '', referenceTime: getMostRecentIntervalTime('30min') },
          '1h': { value: '', referenceTime: getMostRecentIntervalTime('1h') },
          '4h': { value: '', referenceTime: getMostRecentIntervalTime('4h') }
        });
      } else {
        setError(response.data.error || 'Failed to add token');
      }
    } catch (err) {
      if (isDev) console.error('Error adding token:', err);
      setError(err.response?.data?.error || 'Failed to add token. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // Get DexScreener chart URL
  const getDexscreenerChartUrl = () => {
    if (!tokenAddress) return '';
    
    // If we have token info with pairAddress, use that for a more accurate chart
    if (tokenInfo && tokenInfo.pairAddress) {
      return `https://dexscreener.com/solana/${tokenInfo.pairAddress}?embed=1&theme=dark`;
    }
    
    // Fallback to token address
    return `https://dexscreener.com/solana/${tokenAddress}?embed=1&theme=dark`;
  };

  return (
    <div className="token-add-container">
      <div className="token-add-background"></div>
      <Container fluid className="px-4">


        {error && (
          <Alert variant="danger" className="token-add-error">
            <i className="fas fa-exclamation-triangle me-2"></i>
            {error}
          </Alert>
        )}

        {success && (
          <Alert variant="success" className="token-add-success">
            <i className="fas fa-check-circle me-2"></i>
            {success}
          </Alert>
        )}

        <Form onSubmit={handleSubmit} className="token-add-form">
          <Row>
            {/* Left Column - Token Info */}
            <Col lg={6}>
              <div className="form-section">
                <h2>Token Information</h2>
                
                <Form.Group className="mb-4">
                  <Form.Label>Token Address</Form.Label>
                  <Form.Control
                    type="text"
                    value={tokenAddress}
                    onChange={(e) => handleTokenAddressChange(e.target.value)}
                    placeholder="Enter Solana token address (e.g., So11111111111111111111111111111111111111112)"
                    className="token-add-input"
                    required
                  />
                </Form.Group>

                <Form.Group className="mb-4">
                  <Form.Label>Pair Address</Form.Label>
                  <Form.Control
                    type="text"
                    value={pairAddress}
                    onChange={(e) => setPairAddress(e.target.value)}
                    placeholder="Enter pair address (e.g., 4w2cysotX6czaUGmmWg13hDpY4QEMG2CzeKYEQyK9Ama)"
                    className="token-add-input"
                    required
                  />
                </Form.Group>

                  <Form.Group className="mb-4">
                    <Form.Label>Added By</Form.Label>
                    <Form.Control
                      type="text"
                      value={addedBy}
                      onChange={(e) => setAddedBy(e.target.value)}
                      placeholder="Enter your identifier (e.g., admin@example.com)"
                      className="token-add-input"
                      required
                    />
                  </Form.Group>

                  <Form.Group className="mb-4">
                    <Form.Label>Timeframes</Form.Label>
                    <div className="timeframes-selection">
                      {['15m', '30min', '1h', '2h', '4h', '6h', '12h', '1d'].map((tf) => (
                        <Form.Check
                          key={tf}
                          type="checkbox"
                          id={`timeframe-${tf}`}
                          label={tf.toUpperCase()}
                          checked={timeframes.includes(tf)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setTimeframes([...timeframes, tf]);
                            } else {
                              setTimeframes(timeframes.filter(t => t !== tf));
                            }
                          }}
                          className="timeframe-checkbox"
                        />
                      ))}
                    </div>
                    <Form.Text className="text-muted">
                      Select timeframes for token tracking. Default: 30MIN, 1H, 4H
                    </Form.Text>
                  </Form.Group>


                {tokenInfo && (
                  <div className="token-info-card">
                    <h3>Token Details</h3>
                    <div className="token-details">
                      <div className="detail-item">
                        <span className="label">Name:</span>
                        <span className="value">{tokenInfo.name}</span>
                      </div>
                      <div className="detail-item">
                        <span className="label">Symbol:</span>
                        <span className="value">{tokenInfo.symbol}</span>
                      </div>
                      {pairAge !== null && (
                        <div className="detail-item">
                          <span className="label">Pair Age:</span>
                          <span className="value">
                            {pairAge.toFixed(1)} days
                            {isOldToken && (
                              <Badge bg="warning" className="ms-2 old-token-badge">
                                Old Token - EMA Required
                              </Badge>
                            )}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>

              {/* DexScreener Chart */}
              {tokenAddress && (
                <div className="chart-section">
                  <h2>Price Chart</h2>
                  <div className="dexscreener-chart-container">
                    <iframe
                      src={getDexscreenerChartUrl()}
                      width="100%"
                      height="600"
                      frameBorder="0"
                      title="Dexscreener Chart"
                      className="dexscreener-chart"
                    />
                  </div>
                </div>
              )}
            </Col>

            {/* Right Column - AVWAP and EMA Fields */}
            <Col lg={6}>
              {/* AVWAP Section - Required for all tokens */}
              <div className="form-section avwap-section">
                <div className="avwap-header">
                  <h2>AVWAP Configuration</h2>
                  <Badge bg="primary" className="avwap-indicator">
                    <i className="fas fa-chart-line me-1"></i>
                    Required for All Tokens
                  </Badge>
                </div>
                <p className="avwap-description">
                  Anchored Volume Weighted Average Price values are required for all tokens to establish reference points.
                </p>

                {/* AVWAP Configuration by Timeframe */}
                {['30min', '1h', '4h'].map((timeframe) => (
                  <div key={`avwap-timeframe-${timeframe}`} className="avwap-group">
                    <h3>{timeframe.toUpperCase()}</h3>
                    
                    <div className="avwap-row">
                      <div className="avwap-type-label">AVWAP:</div>
                      <Row>
                        <Col md={6}>
                          <Form.Group>
                            <Form.Label>Value</Form.Label>
                            <Form.Control
                              type="number"
                              step="0.000001"
                              value={avwapData[timeframe].value}
                              onChange={(e) => handleAVWAPChange(timeframe, 'value', e.target.value)}
                              placeholder="Enter AVWAP value"
                              className="token-add-input avwap-input"
                              required
                            />
                          </Form.Group>
                        </Col>
                        <Col md={6}>
                          <Form.Group>
                            <Form.Label>Reference Time</Form.Label>
                            <Form.Select
                              value={avwapData[timeframe].referenceTime}
                              onChange={(e) => handleAVWAPChange(timeframe, 'referenceTime', e.target.value)}
                              className="token-add-input avwap-input"
                              required
                            >
                              {generateTimeOptions(timeframe).map((time) => (
                                <option key={time} value={time}>
                                  {time}
                                </option>
                              ))}
                            </Form.Select>
                          </Form.Group>
                        </Col>
                      </Row>
                    </div>
                  </div>
                ))}
              </div>

              {showEMAFields && (
                <div className="form-section ema-section">
                  <div className="ema-header">
                    <h2>EMA Configuration</h2>
                    <Badge bg="info" className="old-token-indicator">
                      <i className="fas fa-clock me-1"></i>
                      Old Token Detected
                    </Badge>
                  </div>
                  <p className="ema-description">
                    This token is older than 7 days. Please provide EMA values for accurate tracking.
                  </p>

                  {/* EMA Configuration by Timeframe */}
                  {['30min', '1h', '4h'].map((timeframe) => (
                    <div key={`timeframe-${timeframe}`} className="ema-group">
                      <h3>{timeframe.toUpperCase()}</h3>
                      
                      {/* EMA21 Row */}
                      <div className="ema-row">
                        <div className="ema-type-label">EMA 21:</div>
                        <Row>
                          <Col md={6}>
                            <Form.Group>
                              <Form.Label>Value</Form.Label>
                              <Form.Control
                                type="number"
                                step="0.000001"
                                value={emaData.ema21[timeframe].value}
                                onChange={(e) => handleEMAChange('ema21', timeframe, 'value', e.target.value)}
                                placeholder="Enter EMA21 value"
                                className="token-add-input ema-input"
                                required={isOldToken}
                              />
                            </Form.Group>
                          </Col>
                          <Col md={6}>
                            <Form.Group>
                              <Form.Label>Reference Time</Form.Label>
                              <Form.Select
                                value={emaData.ema21[timeframe].referenceTime}
                                onChange={(e) => handleEMAChange('ema21', timeframe, 'referenceTime', e.target.value)}
                                className="token-add-input ema-input"
                                required={isOldToken}
                              >
                                {generateTimeOptions(timeframe).map((time) => (
                                  <option key={time} value={time}>
                                    {time}
                                  </option>
                                ))}
                              </Form.Select>
                            </Form.Group>
                          </Col>
                        </Row>
                      </div>

                      {/* EMA34 Row */}
                      <div className="ema-row">
                        <div className="ema-type-label">EMA 34:</div>
                        <Row>
                          <Col md={6}>
                            <Form.Group>
                              <Form.Label>Value</Form.Label>
                              <Form.Control
                                type="number"
                                step="0.000001"
                                value={emaData.ema34[timeframe].value}
                                onChange={(e) => handleEMAChange('ema34', timeframe, 'value', e.target.value)}
                                placeholder="Enter EMA34 value"
                                className="token-add-input ema-input"
                                required={isOldToken}
                              />
                            </Form.Group>
                          </Col>
                          <Col md={6}>
                            <Form.Group>
                              <Form.Label>Reference Time</Form.Label>
                              <Form.Select
                                value={emaData.ema34[timeframe].referenceTime}
                                onChange={(e) => handleEMAChange('ema34', timeframe, 'referenceTime', e.target.value)}
                                className="token-add-input ema-input"
                                required={isOldToken}
                              >
                                {generateTimeOptions(timeframe).map((time) => (
                                  <option key={time} value={time}>
                                    {time}
                                  </option>
                                ))}
                              </Form.Select>
                            </Form.Group>
                          </Col>
                        </Row>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* Submit Section */}
              <div className="submit-section">
                <Button
                  type="submit"
                  disabled={loading || !tokenAddress || !pairAddress || !addedBy}
                  className="token-add-submit-btn"
                  size="lg"
                >
                  {loading ? (
                    <>
                      <Spinner animation="border" size="sm" className="me-2" />
                      Adding Token...
                    </>
                  ) : (
                    <>
                      <i className="fas fa-plus me-2"></i>
                      Add Token to Trading
                    </>
                  )}
                </Button>
              </div>
            </Col>
          </Row>
        </Form>

      </Container>
    </div>
  );
};

export default TokenAddPage;
