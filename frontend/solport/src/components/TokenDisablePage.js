import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Spinner, Alert, Badge, Table } from 'react-bootstrap';
import './TokenDisablePage.css';
import { API_BASE_URL } from '../services/api';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TokenDisablePage = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [tokenAddress, setTokenAddress] = useState('');
  const [reason, setReason] = useState('obscurainvera');
  const [disabledBy, setDisabledBy] = useState('obscurainvera');
  const [tokenInfo, setTokenInfo] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [activeTokens, setActiveTokens] = useState([]);
  const [showTokenList, setShowTokenList] = useState(false);

  // Fetch active tokens for selection
  const fetchActiveTokens = async () => {
    try {
      setSearchLoading(true);
      setError(null);

      const response = await axios.get(`${API_BASE_URL}/api/tokens/list?status=active&limit=100`);
      
      if (response.data && response.data.success) {
        setActiveTokens(response.data.tokens);
        setShowTokenList(true);
      } else {
        setError('Failed to fetch active tokens');
      }
    } catch (err) {
      if (isDev) console.error('Error fetching active tokens:', err);
      setError(err.response?.data?.error || 'Failed to fetch active tokens. Please try again.');
    } finally {
      setSearchLoading(false);
    }
  };

  // Load active tokens on component mount
  useEffect(() => {
    fetchActiveTokens();
  }, []);

  // Handle token address change and fetch token info
  const handleTokenAddressChange = async (address) => {
    setTokenAddress(address);
    setTokenInfo(null);
    setError(null);

    if (!address || address.length < 32) return;

    try {
      setSearchLoading(true);
      
      // First try to get token info from the list of active tokens
      const foundToken = activeTokens.find(token => 
        token.tokenAddress.toLowerCase() === address.toLowerCase()
      );
      
      if (foundToken) {
        setTokenInfo(foundToken);
        return;
      }

      // If not found in active tokens, try to get from DexScreener
      const response = await axios.get(`${API_BASE_URL}/api/price/token/${address}`);
      
      if (response.data && response.data.status === 'success' && response.data.data) {
        const tokenData = response.data.data;
        setTokenInfo({
          symbol: tokenData.symbol,
          name: tokenData.name,
          tokenAddress: address,
          pairAddress: tokenData.pairAddress || '',
          status: 'unknown'
        });
      }
    } catch (err) {
      if (isDev) console.error('Error fetching token info:', err);
      // Don't show error for partial addresses
      if (address.length >= 32) {
        setError('Token not found in active tokens or unable to fetch token information.');
      }
    } finally {
      setSearchLoading(false);
    }
  };

  // Handle token selection from list
  const handleTokenSelect = (token) => {
    setTokenAddress(token.tokenAddress);
    setTokenInfo(token);
    setShowTokenList(false);
    setError(null);
  };

  // Validate form data
  const validateForm = () => {
    if (!tokenAddress.trim()) {
      setError('Token address is required');
      return false;
    }

    if (!reason.trim()) {
      setError('Reason for disabling is required');
      return false;
    }

    if (!disabledBy.trim()) {
      setError('Disabled by field is required');
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
        reason: reason.trim(),
        disabledBy: disabledBy.trim()
      };

      const response = await axios.post(`${API_BASE_URL}/api/tokens/disable`, requestData);

      if (response.data && response.data.success) {
        setSuccess(`Token ${response.data.symbol} disabled successfully!`);
        // Reset form
        setTokenAddress('');
        setReason('obscurainvera');
        setDisabledBy('obscurainvera');
        setTokenInfo(null);
        // Refresh active tokens list
        fetchActiveTokens();
      } else {
        setError(response.data.error || 'Failed to disable token');
      }
    } catch (err) {
      if (isDev) console.error('Error disabling token:', err);
      setError(err.response?.data?.error || 'Failed to disable token. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // Copy to clipboard
  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text).then(() => {
      setSuccess('Copied to clipboard!');
      setTimeout(() => setSuccess(null), 2000);
    });
  };

  // Get DexScreener URL
  const getDexscreenerUrl = (pairAddress) => {
    return `https://dexscreener.com/solana/${pairAddress}`;
  };

  return (
    <div className="token-disable-container">
      <div className="token-disable-background"></div>
      <Container fluid className="px-4">

        {/* Header Section */}
        <div className="token-disable-header">
          <div className="token-disable-title">
            <h1>Disable Token</h1>
            <p className="token-disable-subtitle">
              Disable token tracking and remove from active monitoring
            </p>
          </div>
        </div>

        {/* Error and Success Alerts */}
        {error && (
          <Alert variant="danger" className="token-disable-error">
            <i className="fas fa-exclamation-triangle me-2"></i>
            {error}
          </Alert>
        )}

        {success && (
          <Alert variant="success" className="token-disable-success">
            <i className="fas fa-check-circle me-2"></i>
            {success}
          </Alert>
        )}

        <Form onSubmit={handleSubmit} className="token-disable-form">
          <Row>
            {/* Left Column - Token Selection */}
            <Col lg={6}>
              <div className="form-section">
                <h2>Token Selection</h2>
                
                <Form.Group className="mb-4">
                  <Form.Label>Token Address</Form.Label>
                  <Form.Control
                    type="text"
                    value={tokenAddress}
                    onChange={(e) => handleTokenAddressChange(e.target.value)}
                    placeholder="Enter Solana token address or select from list below"
                    className="token-disable-input"
                    required
                  />
                  {searchLoading && (
                    <div className="search-loading">
                      <Spinner animation="border" size="sm" className="me-2" />
                      Searching for token...
                    </div>
                  )}
                </Form.Group>

                {/* Active Tokens List */}
                <div className="active-tokens-section">
                  <div className="tokens-list-header">
                    <h3>Active Tokens</h3>
                    <Button
                      variant="outline-light"
                      size="sm"
                      onClick={fetchActiveTokens}
                      disabled={searchLoading}
                      className="refresh-tokens-btn"
                    >
                      {searchLoading ? (
                        <Spinner animation="border" size="sm" className="me-1" />
                      ) : (
                        <i className="fas fa-sync-alt me-1"></i>
                      )}
                      Refresh
                    </Button>
                  </div>

                  {showTokenList && (
                    <div className="tokens-list-container">
                      <Table responsive className="tokens-list-table">
                        <thead>
                          <tr>
                            <th>Token</th>
                            <th>Address</th>
                            <th>Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {activeTokens.slice(0, 10).map((token) => (
                            <tr key={token.tokenId}>
                              <td>
                                <div className="token-info">
                                  <div className="token-symbol">{token.symbol}</div>
                                  <div className="token-name">{token.name}</div>
                                </div>
                              </td>
                              <td>
                                <span 
                                  className="token-address"
                                  onClick={() => copyToClipboard(token.tokenAddress)}
                                  title="Click to copy"
                                >
                                  {token.tokenAddress.substring(0, 8)}...{token.tokenAddress.substring(token.tokenAddress.length - 8)}
                                  <i className="fas fa-copy ms-1"></i>
                                </span>
                              </td>
                              <td>
                                <Button
                                  variant="outline-info"
                                  size="sm"
                                  onClick={() => handleTokenSelect(token)}
                                  className="select-token-btn"
                                >
                                  Select
                                </Button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </Table>
                      {activeTokens.length > 10 && (
                        <div className="tokens-list-footer">
                          <small className="text-muted">
                            Showing first 10 of {activeTokens.length} active tokens
                          </small>
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {/* Token Info Card */}
                {tokenInfo && (
                  <div className="token-info-card">
                    <h3>Token Details</h3>
                    <div className="token-details">
                      <div className="detail-item">
                        <span className="label">Symbol:</span>
                        <span className="value">{tokenInfo.symbol}</span>
                      </div>
                      <div className="detail-item">
                        <span className="label">Name:</span>
                        <span className="value">{tokenInfo.name}</span>
                      </div>
                      <div className="detail-item">
                        <span className="label">Token Address:</span>
                        <span 
                          className="value address-value"
                          onClick={() => copyToClipboard(tokenInfo.tokenAddress)}
                          title="Click to copy"
                        >
                          {tokenInfo.tokenAddress.substring(0, 12)}...{tokenInfo.tokenAddress.substring(tokenInfo.tokenAddress.length - 12)}
                          <i className="fas fa-copy ms-1"></i>
                        </span>
                      </div>
                      {tokenInfo.pairAddress && (
                        <div className="detail-item">
                          <span className="label">Pair Address:</span>
                          <span 
                            className="value address-value"
                            onClick={() => copyToClipboard(tokenInfo.pairAddress)}
                            title="Click to copy"
                          >
                            {tokenInfo.pairAddress.substring(0, 12)}...{tokenInfo.pairAddress.substring(tokenInfo.pairAddress.length - 12)}
                            <i className="fas fa-copy ms-1"></i>
                          </span>
                        </div>
                      )}
                      <div className="detail-item">
                        <span className="label">Status:</span>
                        <span className="value">
                          <Badge bg="success" className="status-badge">
                            Active
                          </Badge>
                        </span>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </Col>

            {/* Right Column - Disable Form */}
            <Col lg={6}>
              <div className="form-section">
                <h2>Disable Information</h2>
                
                <Form.Group className="mb-4">
                  <Form.Label>Reason for Disabling</Form.Label>
                  <Form.Control
                    as="textarea"
                    rows={4}
                    value={reason}
                    onChange={(e) => setReason(e.target.value)}
                    placeholder="Enter detailed reason for disabling this token (e.g., Low volume, Poor performance, Security concerns, etc.)"
                    className="token-disable-input"
                    required
                  />
                </Form.Group>

                <Form.Group className="mb-4">
                  <Form.Label>Disabled By</Form.Label>
                  <Form.Control
                    type="text"
                    value={disabledBy}
                    onChange={(e) => setDisabledBy(e.target.value)}
                    placeholder="Enter your identifier (e.g., admin@example.com)"
                    className="token-disable-input"
                    required
                  />
                </Form.Group>


                {/* Submit Section */}
                <div className="submit-section">
                  <Button
                    type="submit"
                    disabled={loading || !tokenAddress || !reason || !disabledBy}
                    className="token-disable-submit-btn"
                    size="lg"
                  >
                    {loading ? (
                      <>
                        <Spinner animation="border" size="sm" className="me-2" />
                        Disabling Token...
                      </>
                    ) : (
                      <>
                        <i className="fas fa-ban me-2"></i>
                        Disable Token
                      </>
                    )}
                  </Button>
                </div>
              </div>
            </Col>
          </Row>
        </Form>

      </Container>
    </div>
  );
};

export default TokenDisablePage;