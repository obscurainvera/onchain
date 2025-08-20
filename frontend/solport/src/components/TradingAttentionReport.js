import React, { useState, useEffect, useMemo } from 'react';
import axios from 'axios';
import { Container, Row, Col, Form, Button, Spinner, Alert } from 'react-bootstrap';
import TradingAttentionTable from './TradingAttentionTable';
import './TradingAttentionReport.css';

import { API_BASE_URL } from '../services/api';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TradingAttentionReport = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [tradingAttentionData, setTradingAttentionData] = useState([]);
  const [filters, setFilters] = useState({
    tokenSearch: '',
    recordLimit: 'all'
  });

  useEffect(() => {
    fetchTradingAttentionData();
  }, []);

  const fetchTradingAttentionData = async () => {
    setLoading(true);
    setError(null);
    try {
      if (isDev) console.log('Fetching trading attention data from:', `${API_BASE_URL}/api/reports/tradingattention`);
      
      const response = await axios.get(`${API_BASE_URL}/api/reports/tradingattention`);
      
      if (isDev) {
        console.log('API Response:', response);
        console.log('Response data:', response.data);
      }
      
      // Check for API error response
      if (response.data.status === 'error') {
        throw new Error(response.data.message || 'Failed to load trading attention data');
      }
      
      // Extract data from the standardized response format
      const responseData = response.data.status === 'success' && response.data.data 
        ? response.data.data 
        : response.data;
        
      if (isDev) console.log('Processed response data:', responseData);
      setTradingAttentionData(responseData || []);
      
    } catch (err) {
      if (isDev) console.error('Error fetching trading attention data:', err);
      setError(err.message || 'Failed to load trading attention data. Please try again later.');
    } finally {
      setLoading(false);
    }
  };

  // Filter data based on token search and record limit
  const filteredData = useMemo(() => {
    if (!tradingAttentionData || tradingAttentionData.length === 0) return [];
    
    let filtered = [...tradingAttentionData];
    
    // Filter by token name or token ID
    if (filters.tokenSearch.trim()) {
      const searchTerm = filters.tokenSearch.trim().toLowerCase();
      filtered = filtered.filter(token => 
        (token.name && token.name.toLowerCase().includes(searchTerm)) ||
        (token.token_id && token.token_id.toLowerCase().includes(searchTerm))
      );
    }
    
    // Apply record limit
    if (filters.recordLimit && filters.recordLimit !== 'all') {
      const limit = parseInt(filters.recordLimit);
      if (!isNaN(limit) && limit > 0) {
        filtered = filtered.slice(0, limit);
      }
    }
    
    return filtered;
  }, [tradingAttentionData, filters]);

  const handleFilterChange = (e) => {
    const { name, value } = e.target;
    setFilters(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    // Filters are applied automatically through useMemo
  };

  const clearFilters = () => {
    setFilters({
      tokenSearch: '',
      recordLimit: '50'
    });
  };

  const handleRefresh = () => {
    fetchTradingAttentionData();
  };

  const renderFilters = () => {
    return (
      <Form onSubmit={handleSubmit} className="trading-attention-filters compact">
        <Row className="align-items-end">
          <Col md={4}>
            <Form.Group className="mb-0">
              <Form.Label>Search by Token ID or Name</Form.Label>
              <Form.Control
                type="text"
                name="tokenSearch"
                value={filters.tokenSearch}
                onChange={handleFilterChange}
                placeholder="Enter token ID or name..."
                className="trading-attention-input compact"
                size="sm"
              />
            </Form.Group>
          </Col>
          <Col md={2}>
            <Form.Group className="mb-0">
              <Form.Label>Records</Form.Label>
              <Form.Select
                name="recordLimit"
                value={filters.recordLimit}
                onChange={handleFilterChange}
                className="trading-attention-input compact"
                size="sm"
              >
                <option value="10">10</option>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
                <option value="all">All</option>
              </Form.Select>
            </Form.Group>
          </Col>
          <Col md={3}>
            <Form.Group className="mb-0">
              <div className="d-flex gap-1">
                <Button 
                  variant="outline-secondary" 
                  onClick={clearFilters} 
                  className="trading-attention-button-outline compact"
                  size="sm"
                >
                  Clear
                </Button>
                <Button 
                  variant="primary" 
                  onClick={handleRefresh} 
                  className="trading-attention-button compact"
                  disabled={loading}
                  size="sm"
                >
                  {loading ? (
                    <>
                      <Spinner animation="border" size="sm" className="me-1" />
                      Load
                    </>
                  ) : (
                    'Refresh'
                  )}
                </Button>
              </div>
            </Form.Group>
          </Col>
        </Row>
      </Form>
    );
  };

  return (
    <div className="trading-attention-container">
      <div className="trading-attention-background"></div>
      <Container fluid className="px-4">


        {renderFilters()}

        {loading ? (
          <div className="trading-attention-loading">
            <Spinner animation="border" role="status" variant="light" />
            <p>Loading trading attention data...</p>
          </div>
        ) : error ? (
          <Alert variant="danger" className="trading-attention-error">
            <i className="fas fa-exclamation-triangle me-2"></i>
            {error}
          </Alert>
        ) : (
          <div className="trading-attention-content">
            <div className="trading-attention-stats mb-3">
              <span className="stats-text">
                Showing {filteredData.length} of {tradingAttentionData.length} tokens
              </span>
            </div>
            
            <TradingAttentionTable data={filteredData} />
          </div>
        )}
      </Container>
    </div>
  );
};

export default TradingAttentionReport; 