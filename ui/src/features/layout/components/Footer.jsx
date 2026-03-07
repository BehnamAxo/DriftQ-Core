export default function Footer({ tick }) {
  return (
    <footer className="dq-footer">
      <span>DriftQ Dashboard - embedded at :8080/ui</span>
      <span>tick #{tick}</span>
    </footer>
  );
}
