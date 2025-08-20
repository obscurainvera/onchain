import React, { useState, useRef, useEffect } from 'react';
import { Table, Toast, ToastContainer } from 'react-bootstrap';
import './TradingAttentionTable.css';

// Environment detection
const isDev = process.env.NODE_ENV === 'development';

const TradingAttentionTable = ({ data }) => {
  const [hoveredRow, setHoveredRow] = useState(null);
  const [showToast, setShowToast] = useState(false);
  const [toastMessage, setToastMessage] = useState('');
  const tableWrapperRef = useRef(null);
  const [isScrollable, setIsScrollable] = useState(false);
  const [sortConfig, setSortConfig] = useState({
    key: 1, // Day 1 (most recent day)
    direction: 'desc'
  });

  useEffect(() => {
    // Check if table is scrollable
    if (tableWrapperRef.current) {
      const { scrollWidth, clientWidth } = tableWrapperRef.current;
      setIsScrollable(scrollWidth > clientWidth);
    }
  }, [data]);

  // Helper function to copy token ID to clipboard
  const copyTokenId = async (tokenId, tokenName) => {
    try {
      await navigator.clipboard.writeText(tokenId);
      setToastMessage(`Copied ${tokenName} token ID to clipboard`);
      setShowToast(true);
    } catch (err) {
      if (isDev) console.error('Failed to copy token ID:', err);
      setToastMessage('Failed to copy token ID');
      setShowToast(true);
    }
  };

  // Format time from timestamp
  const formatTime = (timeString) => {
    if (!timeString) return '--';
    try {
      let date;
      
      // Handle different time formats
      if (typeof timeString === 'string') {
        // Remove extra spaces and handle various formats
        const cleanTimeString = timeString.trim();
        
        // Try parsing as ISO string first
        if (cleanTimeString.includes('T') || cleanTimeString.includes('-')) {
          date = new Date(cleanTimeString);
        } else {
          // Try parsing as time string directly
          date = new Date(`1970-01-01 ${cleanTimeString}`);
        }
      } else {
        date = new Date(timeString);
      }
      
      if (isNaN(date.getTime())) {
        // If still invalid, try another approach
        if (typeof timeString === 'string' && timeString.includes(':')) {
          const timeParts = timeString.split(':');
          if (timeParts.length >= 2) {
            const hour = parseInt(timeParts[0]);
            const minute = parseInt(timeParts[1]);
            date = new Date();
            date.setHours(hour, minute, 0, 0);
          }
        }
      }
      
      if (isNaN(date.getTime())) return '--';
      
      return date.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: true
      });
    } catch (error) {
      if (isDev) console.error('Error formatting time:', error, 'Input:', timeString);
      return '--';
    }
  };

  // Format price with appropriate decimal places
  const formatPrice = (price) => {
    if (!price || price === 0) return '0.00';
    
    const numPrice = parseFloat(price);
    if (isNaN(numPrice)) return '0.00';
    
    if (numPrice >= 1) {
      return numPrice.toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 6
      });
    } else {
      return numPrice.toFixed(8).replace(/\.?0+$/, '');
    }
  };

  // Format score with appropriate decimal places
  const formatScore = (score) => {
    if (!score && score !== 0) return '0';
    
    const numScore = parseFloat(score);
    if (isNaN(numScore)) return '0';
    
    return numScore.toLocaleString('en-US', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 2
    });
  };

  // Format market cap
  const formatMarketCap = (marketCap, marketCapFormatted) => {
    if (marketCapFormatted) return marketCapFormatted;
    if (!marketCap || marketCap === 0) return '$0';
    
    const num = parseFloat(marketCap);
    if (isNaN(num)) return '$0';
    
    if (num >= 1_000_000_000) {
      return `$${(num / 1_000_000_000).toFixed(1)}B`;
    } else if (num >= 1_000_000) {
      return `$${(num / 1_000_000).toFixed(1)}M`;
    } else if (num >= 1_000) {
      return `$${(num / 1_000).toFixed(1)}K`;
    } else {
      return `$${num.toFixed(0)}`;
    }
  };

  // Format date for column header
  const formatDateHeader = (dateString) => {
    if (!dateString) return 'Unknown';
    try {
      // Handle DD-MM-YYYY format from API
      let date;
      if (dateString.includes('-')) {
        const parts = dateString.split('-');
        if (parts.length === 3) {
          // Assume DD-MM-YYYY format from API
          date = new Date(parts[2], parts[1] - 1, parts[0]);
        } else {
          date = new Date(dateString);
        }
      } else {
        date = new Date(dateString);
      }
      
      if (isNaN(date.getTime())) return 'Unknown';
      
      return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric'
      });
    } catch (error) {
      if (isDev) console.error('Error formatting date header:', error);
      return 'Unknown';
    }
  };

  // Calculate maximum score for a specific day for a token
  const getMaxScoreForDay = (token, dayNumber) => {
    if (!token.attention_data || token.attention_data.length === 0) return 0;
    
    const dayData = token.attention_data.find(d => d.day === dayNumber);
    return dayData && dayData.max_score ? parseFloat(dayData.max_score) : 0;
  };

  // Calculate maximum score across all days for a token (fallback)
  const getMaxScore = (token) => {
    if (!token.attention_data || token.attention_data.length === 0) return 0;
    
    let maxScore = 0;
    token.attention_data.forEach(dayData => {
      if (dayData.max_score && parseFloat(dayData.max_score) > maxScore) {
        maxScore = parseFloat(dayData.max_score);
      }
    });
    
    return maxScore;
  };

  // Handle sorting
  const handleSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  // Get sort icon
  const getSortIcon = (key) => {
    if (sortConfig.key !== key) {
      return <span className="sort-icon">⇅</span>;
    }
    return sortConfig.direction === 'asc' 
      ? <span className="sort-icon active">↑</span> 
      : <span className="sort-icon active">↓</span>;
  };

  // Render day data cell
  const renderDayData = (dayData) => {
    if (!dayData) {
      return (
        <div className="day-data-cell">
          <div className="no-data">No data</div>
        </div>
      );
    }

    const {
      min_score,
      max_score,
      min_score_time,
      max_score_time,
      latest_score,
      latest_score_time,
      low_price,
      high_price,
      low_price_time,
      high_price_time,
      difference
    } = dayData;

    return (
      <div className="day-data-cell">
        <div className="latest-score-line">
          <strong>{formatScore(latest_score || max_score)}</strong>
        </div>
        <div className="latest-score-time">
          [{latest_score_time || '--'}]
        </div>
        <div className="score-line">
          <span className="min-score">
            <strong>{formatScore(min_score)}</strong>
          </span>
          <span className="score-separator"> - </span>
          <span className="max-score">
            <strong>{formatScore(max_score)}</strong>
          </span>
        </div>
        <div className="price-line">
          <span className="low-price">{formatPrice(low_price)}</span>
          <span className="price-separator"> : </span>
          <span className="high-price">{formatPrice(high_price)}</span>
          <span className="price-change">
            <strong>{difference ? ` → ${difference.toFixed(2)}%` : ' → 0%'}</strong>
          </span>
        </div>
      </div>
    );
  };

  // Generate column headers from API response data
  const getColumnHeaders = () => {
    if (!data || data.length === 0) return [];
    
    // Get all unique days from the first token's attention_data
    // (assuming all tokens have the same day structure)
    const firstToken = data[0];
    if (!firstToken.attention_data || firstToken.attention_data.length === 0) {
      return [];
    }
    
    // Extract and sort by day number to ensure correct order
    const sortedDays = [...firstToken.attention_data].sort((a, b) => a.day - b.day);
    
    return sortedDays.map(dayData => ({
      day: dayData.day,
      date: dayData.date,
      header: formatDateHeader(dayData.date)
    }));
  };

  const columnHeaders = getColumnHeaders();

  // Sort data based on current sort configuration
  const sortedData = React.useMemo(() => {
    if (!data || data.length === 0) return [];
    
    const sortableData = [...data];
    
    sortableData.sort((a, b) => {
      let aVal, bVal;
      
      switch (sortConfig.key) {
        case 'name':
          aVal = (a.name || '').toLowerCase();
          bVal = (b.name || '').toLowerCase();
          break;
        case 'marketCap':
          aVal = parseFloat(a.market_cap) || 0;
          bVal = parseFloat(b.market_cap) || 0;
          break;
        default:
          // Check if it's a day number (for day-specific max score sorting)
          if (typeof sortConfig.key === 'number' || !isNaN(parseInt(sortConfig.key))) {
            const dayNumber = typeof sortConfig.key === 'number' ? sortConfig.key : parseInt(sortConfig.key);
            aVal = getMaxScoreForDay(a, dayNumber);
            bVal = getMaxScoreForDay(b, dayNumber);
          } else {
            aVal = 0;
            bVal = 0;
          }
      }
      
      if (typeof aVal === 'string') {
        if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
      } else {
        return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
      }
    });
    
    return sortableData;
  }, [data, sortConfig]);

  // If there's no data, show an empty state
  if (!data || data.length === 0) {
    return (
      <div className="trading-attention-empty-state">
        <div className="empty-icon">📊</div>
        <h3>No Trading Attention Data Available</h3>
        <p>Try adjusting your filters or check back later when more data is available.</p>
      </div>
    );
  }

  if (isDev) {
    console.log('TradingAttentionTable data:', data);
    console.log('Column headers:', columnHeaders);
  }

  return (
    <>
      <div 
        ref={tableWrapperRef} 
        className={`trading-attention-table-wrapper ${isScrollable ? 'scrollable' : ''}`}
      >
        <table className="trading-attention-data-table">
          <thead>
            <tr>
              <th 
                className="token-name-header sortable" 
                onClick={() => handleSort('name')}
              >
                <div className="th-content">
                  Token {getSortIcon('name')}
                </div>
              </th>
              <th 
                className="market-cap-header sortable" 
                onClick={() => handleSort('marketCap')}
              >
                <div className="th-content">
                  MC {getSortIcon('marketCap')}
                </div>
              </th>
              {columnHeaders.map((col, index) => (
                <th 
                  key={`day-${col.day}`} 
                  className="day-header sortable"
                  onClick={() => handleSort(col.day)}
                  title={`Sort by max score for ${col.header}`}
                >
                  <div className="th-content">
                    {col.header} {getSortIcon(col.day)}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedData.map((token, index) => (
              <tr
                key={token.token_id || index}
                className={`data-row ${hoveredRow === index ? 'hovered' : ''}`}
                onMouseEnter={() => setHoveredRow(index)}
                onMouseLeave={() => setHoveredRow(null)}
              >
                <td className="token-name-cell">
                  <div className="token-name-content">
                    <span className="token-name">{token.name || 'Unknown Token'}</span>
                    <button 
                      className="copy-button"
                      onClick={() => copyTokenId(token.token_id, token.name)}
                      title="Copy Token ID"
                    >
                      📋
                    </button>
                  </div>
                </td>
                <td className="market-cap-cell">
                  <span className="market-cap-value">
                    {formatMarketCap(token.market_cap, token.market_cap_formatted)}
                  </span>
                </td>
                {columnHeaders.map((col) => {
                  // Find the corresponding day data
                  const dayData = token.attention_data 
                    ? token.attention_data.find(d => d.day === col.day)
                    : null;
                  
                  return (
                    <td key={`${token.token_id}-day-${col.day}`} className="day-data-cell-container">
                      {renderDayData(dayData)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
        
        {sortedData.length > 0 && (
          <div className="table-footer">
            <div className="table-info">
              {sortedData.length} {sortedData.length === 1 ? 'token' : 'tokens'} • 7-day trading attention analysis
            </div>
          </div>
        )}
      </div>

      {/* Toast for copy feedback */}
      <ToastContainer position="top-end" className="p-3">
        <Toast
          show={showToast}
          onClose={() => setShowToast(false)}
          delay={3000}
          autohide
          className="copy-toast"
        >
          <Toast.Body>
            <i className="fas fa-check-circle me-2"></i>
            {toastMessage}
          </Toast.Body>
        </Toast>
      </ToastContainer>
    </>
  );
};

export default TradingAttentionTable; 