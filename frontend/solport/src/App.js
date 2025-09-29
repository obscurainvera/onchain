import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, NavLink } from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.min.css';
import PortfolioCalculator from './components/PortfolioCalculator';
import TokenAddPage from './components/TokenAddPage';
import TokenListPage from './components/TokenListPage';
import TokenDisablePage from './components/TokenDisablePage';
import TokenEnablePage from './components/TokenEnablePage';
import './App.css';
import { FaCoins } from 'react-icons/fa';

function App() {
  return (
    <Router>
      <div className="App">
        <header className="App-header">
          <div className="logo">
            <Link to="/">
              <FaCoins className="logo-icon" />
              <span className="logo-text">SOL <span className="logo-highlight">PORT</span></span>
            </Link>
          </div>
          <nav>
            <NavLink to="/calculator" className={({ isActive }) => isActive ? "App-link active" : "App-link"}>Calculator</NavLink>
            <NavLink to="/addtoken" className={({ isActive }) => isActive ? "App-link active" : "App-link"}>Add Token</NavLink>
            <NavLink to="/tokens" className={({ isActive }) => isActive ? "App-link active" : "App-link"}>Token List</NavLink>
            <NavLink to="/disabletoken" className={({ isActive }) => isActive ? "App-link active" : "App-link"}>Disable Token</NavLink>
            <NavLink to="/enabletoken" className={({ isActive }) => isActive ? "App-link active" : "App-link"}>Enable Token</NavLink>
          </nav>
        </header>
        <main className="container fade-in">
          <Routes>
            <Route path="/" element={<TokenListPage />} />
            <Route path="/calculator" element={<PortfolioCalculator />} />
            <Route path="/addtoken" element={<TokenAddPage />} />
            <Route path="/tokens" element={<TokenListPage />} />
            <Route path="/disabletoken" element={<TokenDisablePage />} />
            <Route path="/enabletoken" element={<TokenEnablePage />} />
            <Route path="/tokenmetrics" element={
              <div className="coming-soon">
                <h2>Token Metrics</h2>
                <p>In-depth token metrics and analysis tools are currently under development. Check back soon for updates!</p>
              </div>
            } />
            <Route path="/allocation" element={
              <div className="coming-soon">
                <h2>Portfolio Allocation</h2>
                <p>Visual breakdowns of your portfolio allocation across different tokens and chains will be available here soon.</p>
              </div>
            } />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
