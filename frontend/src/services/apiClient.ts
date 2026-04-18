import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import { isValidJWT, isTokenExpired } from '../utils/security';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8081';

// Validate configuration
if (!import.meta.env.VITE_API_BASE_URL && import.meta.env.PROD) {
  console.error('VITE_API_BASE_URL not configured for production');
}

class ApiClient {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: API_BASE_URL,
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor
    this.client.interceptors.request.use(
      (config) => {
        // Add auth token if available and valid
        const token = localStorage.getItem('auth_token');
        if (token && isValidJWT(token) && !isTokenExpired(token)) {
          config.headers.Authorization = `Bearer ${token}`;
        } else if (token) {
          // Remove invalid/expired token
          localStorage.removeItem('auth_token');
          console.warn('Invalid or expired auth token removed');
        }
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response interceptor
    this.client.interceptors.response.use(
      (response) => response.data,
      (error) => {
        console.error('API Error:', error.response?.data || error.message);
        return Promise.reject(error);
      }
    );
  }

  async get<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
    return this.client.get<T, T>(url, config);
  }

  async post<T>(url: string, data?: unknown, config?: AxiosRequestConfig): Promise<T> {
    return this.client.post<T, T>(url, data, config);
  }

  async put<T>(url: string, data?: unknown, config?: AxiosRequestConfig): Promise<T> {
    return this.client.put<T, T>(url, data, config);
  }

  async delete<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
    return this.client.delete<T, T>(url, config);
  }
}

export const apiClient = new ApiClient();
