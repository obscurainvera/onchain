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
  const [addedBy, setAddedBy] = useState('obscurainvera');
  const [timeframes, setTimeframes] = useState(['1h', '4h']);


  // Check if token address is valid and fetch token info
  const handleTokenAddressChange = async (address) => {
    setTokenAddress(address);
    setTokenInfo(null);
    setPairAge(null);
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
        timeframes: timeframes
      };

      const response = await axios.post(`${API_BASE_URL}/api/tokens/add`, requestData);

      if (response.data.success) {
        setSuccess(`Token ${tokenInfo?.symbol || 'successfully'} added to tracking!`);
        // Reset form
        setTokenAddress('');
        setPairAddress('');
        setAddedBy('obscurainvera');
        setTimeframes(['1h', '4h']);
        setTokenInfo(null);
        setPairAge(null);
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
                      Select timeframes for token tracking. Default: 1H, 4H
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

            {/* Right Column - Submit Section */}
            <Col lg={6}>
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
