import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Spinner, Alert, Badge, Table } from 'react-bootstrap';
import './TokenDeletePage.css';
import { API_BASE_URL } from '../services/api';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TokenDeletePage = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [tokenAddress, setTokenAddress] = useState('');
  const [deleteConfirmation, setDeleteConfirmation] = useState('');
  const [deletedBy, setDeletedBy] = useState('obscurainvera');
  const [tokenInfo, setTokenInfo] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [activeTokens, setActiveTokens] = useState([]);
  const [showTokenList, setShowTokenList] = useState(false);

  // Fetch active and disabled tokens for selection
  const fetchAllTokens = async () => {
    try {
      setSearchLoading(true);
      setError(null);

      const response = await axios.get(`${API_BASE_URL}/api/tokens/list?status=all&limit=100`);
      
      if (response.data && response.data.success) {
        setActiveTokens(response.data.tokens);
        setShowTokenList(true);
      } else {
        setError('Failed to fetch tokens');
      }
    } catch (err) {
      if (isDev) console.error('Error fetching tokens:', err);
      setError(err.response?.data?.error || 'Failed to fetch tokens. Please try again.');
    } finally {
      setSearchLoading(false);
    }
  };

  // Load tokens on component mount
  useEffect(() => {
    fetchAllTokens();
  }, []);

  // Handle token address change and fetch token info
  const handleTokenAddressChange = async (address) => {
    setTokenAddress(address);
    setTokenInfo(null);
    setError(null);
    setDeleteConfirmation('');

    if (!address || address.length < 32) return;

    try {
      setSearchLoading(true);
      
      // Try to get token info from the list of tokens
      const foundToken = activeTokens.find(token => 
        token.tokenAddress.toLowerCase() === address.toLowerCase()
      );
      
      if (foundToken) {
        setTokenInfo(foundToken);
        return;
      }

      // If not found in tokens list, try to get from DexScreener
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
        setError('Token not found in database or unable to fetch token information.');
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
    setDeleteConfirmation('');
  };

  // Validate form data
  const validateForm = () => {
    if (!tokenAddress.trim()) {
      setError('Token address is required');
      return false;
    }

    if (!tokenInfo) {
      setError('Token information not found. Please select a valid token.');
      return false;
    }

    if (deleteConfirmation !== tokenInfo.symbol) {
      setError(`You must type "${tokenInfo.symbol}" exactly to confirm deletion`);
      return false;
    }

    if (!deletedBy.trim()) {
      setError('Deleted by field is required');
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
        deletedBy: deletedBy.trim()
      };

      const response = await axios.post(`${API_BASE_URL}/api/tokens/delete`, requestData);

      if (response.data && response.data.success) {
        const records = response.data.recordsDeleted;
        const breakdown = [
          `Alerts: ${records.alerts}`,
          `RSI States: ${records.rsiStates}`,
          `AVWAP States: ${records.avwapStates}`,
          `VWAP Sessions: ${records.vwapSessions}`,
          `EMA States: ${records.emaStates}`,
          `OHLCV Details: ${records.ohlcvDetails}`,
          `Timeframes: ${records.timeframeMetadata}`,
          `Token: ${records.trackedTokens}`
        ].join(', ');
        
        setSuccess(
          `Token ${response.data.symbol} permanently deleted! Records removed: ${breakdown}`
        );
        
        // Reset form
        setTokenAddress('');
        setDeleteConfirmation('');
        setDeletedBy('obscurainvera');
        setTokenInfo(null);
        
        // Refresh tokens list
        fetchAllTokens();
      } else {
        setError(response.data.error || 'Failed to delete token');
      }
    } catch (err) {
      if (isDev) console.error('Error deleting token:', err);
      setError(err.response?.data?.error || 'Failed to delete token. Please try again.');
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

  return (
    <div className="token-delete-container">
      <div className="token-delete-background"></div>
      <Container fluid className="px-4">

        {/* Header Section */}
        <div className="token-delete-header">
          <div className="token-delete-title">
            <h1>Delete Token</h1>
            <p className="token-delete-subtitle">
              Permanently remove token and all associated data from the system
            </p>
          </div>
        </div>

        {/* Error and Success Alerts */}
        {error && (
          <Alert variant="danger" className="token-delete-error">
            <i className="fas fa-exclamation-triangle me-2"></i>
            {error}
          </Alert>
        )}

        {success && (
          <Alert variant="success" className="token-delete-success">
            <i className="fas fa-check-circle me-2"></i>
            {success}
          </Alert>
        )}

        <Form onSubmit={handleSubmit} className="token-delete-form">
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
                    className="token-delete-input"
                    required
                  />
                  {searchLoading && (
                    <div className="search-loading">
                      <Spinner animation="border" size="sm" className="me-2" />
                      Searching for token...
                    </div>
                  )}
                </Form.Group>

                {/* All Tokens List */}
                <div className="active-tokens-section">
                  <div className="tokens-list-header">
                    <h3>Available Tokens</h3>
                    <Button
                      variant="outline-light"
                      size="sm"
                      onClick={fetchAllTokens}
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
                            <th>Status</th>
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
                                <Badge 
                                  bg={token.status === 'active' ? 'success' : 'danger'}
                                  className="status-badge"
                                >
                                  {token.status === 'active' ? 'Active' : 'Disabled'}
                                </Badge>
                              </td>
                              <td>
                                <Button
                                  variant="outline-danger"
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
                            Showing first 10 of {activeTokens.length} tokens
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
                          <Badge 
                            bg={tokenInfo.status === 'active' ? 'success' : 'danger'}
                            className="status-badge"
                          >
                            {tokenInfo.status === 'active' ? 'Active' : 'Disabled'}
                          </Badge>
                        </span>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </Col>

            {/* Right Column - Delete Confirmation Form */}
            <Col lg={6}>
              <div className="form-section">
                <h2>Delete Confirmation</h2>
                
                {/* Warning Section */}
                <Alert variant="danger" className="warning-alert">
                  <div className="warning-icon">
                    <i className="fas fa-exclamation-triangle"></i>
                  </div>
                  <div className="warning-content">
                    <h4>⚠️ PERMANENT DELETION WARNING</h4>
                    <p>This action is <strong>IRREVERSIBLE</strong> and will permanently delete:</p>
                    <ul>
                      <li>Token tracking information</li>
                      <li>All historical OHLCV data</li>
                      <li>All EMA, VWAP, AVWAP states</li>
                      <li>All RSI and Stochastic RSI data</li>
                      <li>All alerts and notifications</li>
                      <li>All timeframe metadata</li>
                    </ul>
                    <p className="mb-0"><strong>There is no way to recover this data once deleted.</strong></p>
                  </div>
                </Alert>

                <Form.Group className="mb-4">
                  <Form.Label>
                    Type <span className="text-danger font-weight-bold">{tokenInfo?.symbol || '[TOKEN SYMBOL]'}</span> to confirm deletion
                  </Form.Label>
                  <Form.Control
                    type="text"
                    value={deleteConfirmation}
                    onChange={(e) => setDeleteConfirmation(e.target.value)}
                    placeholder={`Type ${tokenInfo?.symbol || 'token symbol'} exactly to confirm`}
                    className="token-delete-input"
                    required
                    autoComplete="off"
                    disabled={!tokenInfo}
                  />
                  {deleteConfirmation && tokenInfo && deleteConfirmation === tokenInfo.symbol && (
                    <div className="confirmation-valid">
                      <i className="fas fa-check-circle me-2"></i>
                      Confirmation matched! You may proceed.
                    </div>
                  )}
                </Form.Group>

                <Form.Group className="mb-4">
                  <Form.Label>Deleted By</Form.Label>
                  <Form.Control
                    type="text"
                    value={deletedBy}
                    onChange={(e) => setDeletedBy(e.target.value)}
                    placeholder="Enter your identifier (e.g., admin@example.com)"
                    className="token-delete-input"
                    required
                  />
                </Form.Group>

                {/* Submit Section */}
                <div className="submit-section">
                  <Button
                    type="submit"
                    disabled={loading || !tokenAddress || !tokenInfo || deleteConfirmation !== tokenInfo?.symbol || !deletedBy}
                    className="token-delete-submit-btn"
                    size="lg"
                  >
                    {loading ? (
                      <>
                        <Spinner animation="border" size="sm" className="me-2" />
                        Deleting Token...
                      </>
                    ) : (
                      <>
                        <i className="fas fa-trash-alt me-2"></i>
                        Delete Token Permanently
                      </>
                    )}
                  </Button>
                  <p className="submit-warning">
                    By clicking this button, you acknowledge that this action cannot be undone.
                  </p>
                </div>
              </div>
            </Col>
          </Row>
        </Form>

      </Container>
    </div>
  );
};

export default TokenDeletePage;

