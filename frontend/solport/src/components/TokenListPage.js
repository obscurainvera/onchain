import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Spinner, Alert, Badge, Table, Modal, Pagination } from 'react-bootstrap';
import './TokenListPage.css';
import { API_BASE_URL } from '../services/api';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TokenListPage = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [tokens, setTokens] = useState([]);
  const [filteredTokens, setFilteredTokens] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('active');
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [itemsPerPage] = useState(20);
  const [showDisableModal, setShowDisableModal] = useState(false);
  const [selectedToken, setSelectedToken] = useState(null);
  const [disableReason, setDisableReason] = useState('');
  const [disableLoading, setDisableLoading] = useState(false);

  // Fetch tokens from API
  const fetchTokens = async (page = 1, status = 'active') => {
    try {
      setLoading(true);
      setError(null);

      const params = new URLSearchParams({
        status: status,
        limit: itemsPerPage,
        offset: (page - 1) * itemsPerPage
      });

      const response = await axios.get(`${API_BASE_URL}/api/tokens/list?${params}`);
      
      if (response.data && response.data.success) {
        setTokens(response.data.tokens);
        setFilteredTokens(response.data.tokens);
        setTotalCount(response.data.pagination.total);
        setTotalPages(Math.ceil(response.data.pagination.total / itemsPerPage));
        setCurrentPage(page);
      } else {
        setError('Failed to fetch tokens');
      }
    } catch (err) {
      if (isDev) console.error('Error fetching tokens:', err);
      setError(err.response?.data?.error || 'Failed to fetch tokens. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // Load tokens on component mount
  useEffect(() => {
    fetchTokens(1, statusFilter);
  }, [statusFilter]);

  // Filter tokens based on search term
  useEffect(() => {
    if (!searchTerm.trim()) {
      setFilteredTokens(tokens);
    } else {
      const filtered = tokens.filter(token => 
        token.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
        token.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        token.tokenAddress.toLowerCase().includes(searchTerm.toLowerCase()) ||
        token.pairAddress.toLowerCase().includes(searchTerm.toLowerCase())
      );
      setFilteredTokens(filtered);
    }
  }, [searchTerm, tokens]);

  // Handle search input change
  const handleSearchChange = (e) => {
    setSearchTerm(e.target.value);
  };

  // Handle status filter change
  const handleStatusFilterChange = (e) => {
    setStatusFilter(e.target.value);
    setCurrentPage(1);
  };

  // Handle page change
  const handlePageChange = (page) => {
    setCurrentPage(page);
    fetchTokens(page, statusFilter);
  };

  // Handle token disable
  const handleDisableToken = (token) => {
    setSelectedToken(token);
    setDisableReason('');
    setShowDisableModal(true);
  };

  // Confirm token disable
  const confirmDisableToken = async () => {
    if (!selectedToken || !disableReason.trim()) {
      setError('Please provide a reason for disabling the token');
      return;
    }

    try {
      setDisableLoading(true);
      setError(null);

      const response = await axios.post(`${API_BASE_URL}/api/tokens/disable`, {
        tokenAddress: selectedToken.tokenAddress,
        reason: disableReason.trim(),
        disabledBy: 'admin@example.com' // You might want to get this from user context
      });

      if (response.data && response.data.success) {
        setSuccess(`Token ${selectedToken.symbol} disabled successfully`);
        setShowDisableModal(false);
        setSelectedToken(null);
        setDisableReason('');
        // Refresh the token list
        fetchTokens(currentPage, statusFilter);
      } else {
        setError(response.data.error || 'Failed to disable token');
      }
    } catch (err) {
      if (isDev) console.error('Error disabling token:', err);
      setError(err.response?.data?.error || 'Failed to disable token. Please try again.');
    } finally {
      setDisableLoading(false);
    }
  };

  // Format date for display
  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
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
    <div className="token-list-container">
      <div className="token-list-background"></div>
      <Container fluid className="px-4">
        

        {/* Error and Success Alerts */}
        {error && (
          <Alert variant="danger" className="token-list-error">
            <i className="fas fa-exclamation-triangle me-2"></i>
            {error}
          </Alert>
        )}

        {success && (
          <Alert variant="success" className="token-list-success">
            <i className="fas fa-check-circle me-2"></i>
            {success}
          </Alert>
        )}

        {/* Compact Search Section */}
        <div className="compact-search-section">
          <Row className="align-items-end">
            <Col md={4}>
              <Form.Control
                type="text"
                value={searchTerm}
                onChange={handleSearchChange}
                placeholder="Search tokens..."
                className="compact-search-input"
                size="sm"
              />
            </Col>
            <Col md={2}>
              <Form.Select
                value={statusFilter}
                onChange={handleStatusFilterChange}
                className="compact-search-input"
                size="sm"
              >
                <option value="active">Active</option>
                <option value="disabled">Disabled</option>
                <option value="all">All</option>
              </Form.Select>
            </Col>
            <Col md={2}>
              <Button
                variant="outline-light"
                onClick={() => fetchTokens(currentPage, statusFilter)}
                disabled={loading}
                className="compact-refresh-btn"
                size="sm"
              >
                {loading ? (
                  <Spinner animation="border" size="sm" />
                ) : (
                  <i className="fas fa-sync-alt"></i>
                )}
              </Button>
            </Col>
            <Col md={4}>
              <div className="tokens-count">
                <Badge bg="info" className="token-count-badge">
                  {filteredTokens.length} of {totalCount} tokens
                </Badge>
              </div>
            </Col>
          </Row>
        </div>

        {/* Tokens Table Section */}
        <div className="form-section">

          {loading ? (
            <div className="text-center py-5">
              <Spinner animation="border" size="lg" />
              <p className="mt-3">Loading tokens...</p>
            </div>
          ) : filteredTokens.length === 0 ? (
            <div className="text-center py-5">
              <i className="fas fa-coins fa-3x text-muted mb-3"></i>
              <h4>No tokens found</h4>
              <p className="text-muted">
                {searchTerm ? 'Try adjusting your search criteria' : 'No tokens match the selected criteria'}
              </p>
            </div>
          ) : (
            <>
              <div className="tokens-table-container">
                <Table responsive className="tokens-table">
                  <thead>
                    <tr>
                      <th>Token</th>
                      <th>Addresses</th>
                      <th>Status</th>
                      <th>Added</th>
                      <th>Timeframes</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredTokens.map((token) => (
                      <tr key={token.tokenId}>
                        <td>
                          <div className="token-info">
                            <div className="token-symbol">{token.symbol}</div>
                            <div className="token-name">{token.name}</div>
                          </div>
                        </td>
                        <td>
                          <div className="address-info">
                            <div className="address-item">
                              <span className="address-label">Token:</span>
                              <span 
                                className="address-value"
                                onClick={() => copyToClipboard(token.tokenAddress)}
                                title="Click to copy"
                              >
                                {token.tokenAddress.substring(0, 8)}...{token.tokenAddress.substring(token.tokenAddress.length - 8)}
                                <i className="fas fa-copy ms-1"></i>
                              </span>
                            </div>
                            <div className="address-item">
                              <span className="address-label">Pair:</span>
                              <span 
                                className="address-value"
                                onClick={() => copyToClipboard(token.pairAddress)}
                                title="Click to copy"
                              >
                                {token.pairAddress.substring(0, 8)}...{token.pairAddress.substring(token.pairAddress.length - 8)}
                                <i className="fas fa-copy ms-1"></i>
                              </span>
                            </div>
                          </div>
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
                          <div className="date-info">
                            <div className="date-value">{formatDate(token.createdAt)}</div>
                            <div className="date-label">by {token.addedBy}</div>
                          </div>
                        </td>
                        <td>
                          <Badge bg="secondary" className="timeframes-badge">
                            {token.activeTimeframes || 0} timeframes
                          </Badge>
                        </td>
                        <td>
                          <div className="action-buttons">
                            <Button
                              variant="outline-info"
                              size="sm"
                              onClick={() => window.open(getDexscreenerUrl(token.pairAddress), '_blank')}
                              className="action-btn"
                              title="View on DexScreener"
                            >
                              <i className="fas fa-external-link-alt"></i>
                            </Button>
                            {token.status === 'active' && (
                              <Button
                                variant="outline-danger"
                                size="sm"
                                onClick={() => handleDisableToken(token)}
                                className="action-btn"
                                title="Disable Token"
                              >
                                <i className="fas fa-ban"></i>
                              </Button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="pagination-container">
                  <Pagination>
                    <Pagination.First 
                      onClick={() => handlePageChange(1)}
                      disabled={currentPage === 1}
                    />
                    <Pagination.Prev 
                      onClick={() => handlePageChange(currentPage - 1)}
                      disabled={currentPage === 1}
                    />
                    
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      const pageNum = Math.max(1, currentPage - 2) + i;
                      if (pageNum > totalPages) return null;
                      
                      return (
                        <Pagination.Item
                          key={pageNum}
                          active={pageNum === currentPage}
                          onClick={() => handlePageChange(pageNum)}
                        >
                          {pageNum}
                        </Pagination.Item>
                      );
                    })}
                    
                    <Pagination.Next 
                      onClick={() => handlePageChange(currentPage + 1)}
                      disabled={currentPage === totalPages}
                    />
                    <Pagination.Last 
                      onClick={() => handlePageChange(totalPages)}
                      disabled={currentPage === totalPages}
                    />
                  </Pagination>
                </div>
              )}
            </>
          )}
        </div>

        {/* Disable Token Modal */}
        <Modal show={showDisableModal} onHide={() => setShowDisableModal(false)} centered>
          <Modal.Header closeButton>
            <Modal.Title>Disable Token</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <p>Are you sure you want to disable <strong>{selectedToken?.symbol}</strong>?</p>
            <Form.Group>
              <Form.Label>Reason for disabling:</Form.Label>
              <Form.Control
                as="textarea"
                rows={3}
                value={disableReason}
                onChange={(e) => setDisableReason(e.target.value)}
                placeholder="Enter reason for disabling this token..."
                className="token-list-input"
              />
            </Form.Group>
          </Modal.Body>
          <Modal.Footer>
            <Button 
              variant="secondary" 
              onClick={() => setShowDisableModal(false)}
              disabled={disableLoading}
            >
              Cancel
            </Button>
            <Button 
              variant="danger" 
              onClick={confirmDisableToken}
              disabled={disableLoading || !disableReason.trim()}
            >
              {disableLoading ? (
                <>
                  <Spinner animation="border" size="sm" className="me-2" />
                  Disabling...
                </>
              ) : (
                'Disable Token'
              )}
            </Button>
          </Modal.Footer>
        </Modal>

      </Container>
    </div>
  );
};

export default TokenListPage;
