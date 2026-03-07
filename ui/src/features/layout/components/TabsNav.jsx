export default function TabsNav({ tabs, activeTab, onSelect }) {
  return (
    <nav className="dq-tabs">
      {
        tabs.map((tab) => (
          <button key={tab} type="button" className={`dq-tab ${activeTab === tab ? "active" : ""}`} onClick={() => onSelect(tab)}>
            {tab}
          </button>
        ))
      }
    </nav>
  );
}
