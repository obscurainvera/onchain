import axios from 'axios';

// API base URL configuration
import { API_BASE_URL } from './api';

const getStrategyPerformanceConfigs = async (params) => {
  return axios.get(`${API_BASE_URL}/api/reports/strategy-configs`, { params });
};

const getStrategyPerformanceExecutions = async (params) => {
  return axios.get(`${API_BASE_URL}/api/reports/strategy-executions`, { params });
};

const getStrategySpecificExecutions = async (strategyId, params) => {
  return axios.get(`${API_BASE_URL}/api/reports/strategy-config/${strategyId}/executions`, { params });
};

export { getStrategyPerformanceConfigs, getStrategyPerformanceExecutions, getStrategySpecificExecutions }; 