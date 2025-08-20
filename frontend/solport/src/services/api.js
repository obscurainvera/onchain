// API base URL configuration
// Use relative URL for production, localhost for development
const isDevelopment = process.env.NODE_ENV === 'development';
export const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || (isDevelopment ? 'http://localhost:5000' : ''); 