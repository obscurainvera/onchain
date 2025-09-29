import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Spinner, Alert, Badge, Table } from 'react-bootstrap';
import './TokenEnablePage.css';
import { API_BASE_URL } from '../services/api';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TokenEnablePage = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [tokenAddress, setTokenAddress] = useState('');
  const [reason, setReason] = useState('obscurainvera');
  const [enabledBy, setEnabledBy] = useState('obscurainvera');
  const [tokenInfo, setTokenInfo] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [disabledTokens, setDisabledTokens] = useState([]);
  const [showTokenList, setShowTokenList] = useState(false);

  // Fetch disabled tokens for selection
  const fetchDisabledTokens = async () => {
    try {
      setSearchLoading(true);
      setError(null);

      const response = await axios.get(`${API_BASE_URL}/api/tokens/list?status=disabled&limit=100`);
      
      if (response.data && response.data.success) {
        setDisabledTokens(response.data.tokens);
        setShowTokenList(true);
      } else {
        setError('Failed to fetch disabled tokens');
      }
    } catch (err) {
      if (isDev) console.error('Error fetching disabled tokens:', err);
      setError(err.response?.data?.error || 'Failed to fetch disabled tokens. Please try again.');
    } finally {
      setSearchLoading(false);
    }
  };

  // Load disabled tokens on component mount
  useEffect(() => {
    fetchDisabledTokens();
  }, []);

  // Handle token address change and fetch token info
  const handleTokenAddressChange = async (address) => {
    setTokenAddress(address);
    setTokenInfo(null);
    setError(null);

    if (!address || address.length < 32) return;

    try {
      setSearchLoading(true);
      
      // First try to get token info from the list of disabled tokens
      const foundToken = disabledTokens.find(token => 
        token.tokenAddress.toLowerCase() === address.toLowerCase()
      );
      
      if (foundToken) {
        setTokenInfo(foundToken);
        return;
      }

      // If not found in disabled tokens, show error
      setError('Token not found in disabled tokens list.');
    } catch (err) {
      if (isDev) console.error('Error fetching token info:', err);
      if (address.length >= 32) {
        setError('Token not found in disabled tokens list.');
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
      setError('Reason for enabling is required');
      return false;
    }

    if (!enabledBy.trim()) {
      setError('Enabled by field is required');
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
        enabledBy: enabledBy.trim()
      };

      const response = await axios.post(`${API_BASE_URL}/api/tokens/enable`, requestData);

      if (response.data && response.data.success) {
        setSuccess(`Token ${response.data.symbol} enabled successfully!`);
        // Reset form
        setTokenAddress('');
        setReason('obscurainvera');
        setEnabledBy('obscurainvera');
        setTokenInfo(null);
        // Refresh disabled tokens list
        fetchDisabledTokens();
      } else {
        setError(response.data.error || 'Failed to enable token');
      }
    } catch (err) {
      if (isDev) console.error('Error enabling token:', err);
      setError(err.response?.data?.error || 'Failed to enable token. Please try again.');
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
    <div className="token-enable-container">
      <div className="token-enable-background"></div>
      <Container fluid className="px-4">

        {/* Header Section */}
        <div className="token-enable-header">
          <div className="token-enable-title">
            <h1>Enable Token</h1>
            <p className="token-enable-subtitle">
              Re-enable token tracking and restore to active monitoring
            </p>
          </div>
        </div>

        {/* Error and Success Alerts */}
        {error && (
          <Alert variant="danger" className="token-enable-error">
            <i className="fas fa-exclamation-triangle me-2"></i>
            {error}
          </Alert>
        )}

        {success && (
          <Alert variant="success" className="token-enable-success">
            <i className="fas fa-check-circle me-2"></i>
            {success}
          </Alert>
        )}

        <Form onSubmit={handleSubmit} className="token-enable-form">
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
                    className="token-enable-input"
                    required
                  />
                  {searchLoading && (
                    <div className="search-loading">
                      <Spinner animation="border" size="sm" className="me-2" />
                      Searching for token...
                    </div>
                  )}
                </Form.Group>

                {/* Disabled Tokens List */}
                <div className="disabled-tokens-section">
                  <div className="tokens-list-header">
                    <h3>Disabled Tokens</h3>
                    <Button
                      variant="outline-light"
                      size="sm"
                      onClick={fetchDisabledTokens}
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
                            <th>Disabled At</th>
                            <th>Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {disabledTokens.slice(0, 10).map((token) => (
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
                                <span className="disabled-date">
                                  {token.disabledAt ? new Date(token.disabledAt).toLocaleDateString() : 'N/A'}
                                </span>
                              </td>
                              <td>
                                <Button
                                  variant="outline-success"
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
                      {disabledTokens.length > 10 && (
                        <div className="tokens-list-footer">
                          <small className="text-muted">
                            Showing first 10 of {disabledTokens.length} disabled tokens
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
                          <Badge bg="danger" className="status-badge">
                            Disabled
                          </Badge>
                        </span>
                      </div>
                      {tokenInfo.disabledAt && (
                        <div className="detail-item">
                          <span className="label">Disabled At:</span>
                          <span className="value">
                            {new Date(tokenInfo.disabledAt).toLocaleString()}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            </Col>

            {/* Right Column - Enable Form */}
            <Col lg={6}>
              <div className="form-section">
                <h2>Enable Information</h2>
                
                <Form.Group className="mb-4">
                  <Form.Label>Reason for Enabling</Form.Label>
                  <Form.Control
                    as="textarea"
                    rows={4}
                    value={reason}
                    onChange={(e) => setReason(e.target.value)}
                    placeholder="Enter detailed reason for enabling this token (e.g., Volume improved, Performance recovered, Security concerns resolved, etc.)"
                    className="token-enable-input"
                    required
                  />
                </Form.Group>

                <Form.Group className="mb-4">
                  <Form.Label>Enabled By</Form.Label>
                  <Form.Control
                    type="text"
                    value={enabledBy}
                    onChange={(e) => setEnabledBy(e.target.value)}
                    placeholder="Enter your identifier (e.g., admin@example.com)"
                    className="token-enable-input"
                    required
                  />
                </Form.Group>

                {/* Submit Section */}
                <div className="submit-section">
                  <Button
                    type="submit"
                    disabled={loading || !tokenAddress || !reason || !enabledBy}
                    className="token-enable-submit-btn"
                    size="lg"
                  >
                    {loading ? (
                      <>
                        <Spinner animation="border" size="sm" className="me-2" />
                        Enabling Token...
                      </>
                    ) : (
                      <>
                        <i className="fas fa-check me-2"></i>
                        Enable Token
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

export default TokenEnablePage;
