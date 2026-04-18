import { Link, useLocation } from 'react-router-dom';
import { useClickTracker } from '../../hooks/useClickTracker';
import './NavigationBar.css';

export function NavigationBar() {
  const location = useLocation();
  const { trackClick } = useClickTracker('NavigationBar');

  const navItems = [
    { path: '/', label: 'Dashboard' },
    { path: '/sessions', label: 'Sessions' },
    { path: '/pages', label: 'Pages' },
    { path: '/journeys', label: 'Journeys' },
  ];

  return (
    <nav className="navigation-bar">
      <div className="nav-brand">
        <h1>Clickstream Analytics</h1>
      </div>
      <div className="nav-links">
        {navItems.map((item) => (
          <Link
            key={item.path}
            to={item.path}
            className={`nav-link ${location.pathname === item.path ? 'active' : ''}`}
            onClick={() => trackClick(`nav_${item.label.toLowerCase()}`)}
          >
            {item.label}
          </Link>
        ))}
      </div>
    </nav>
  );
}
