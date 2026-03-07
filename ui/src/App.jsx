import ConsumersTab from "./components/tabs/ConsumersTab";
import Controls from "./components/layout/Controls";
import DeadLettersTab from "./components/tabs/DeadLettersTab";
import Footer from "./components/layout/Footer";
import Header from "./components/layout/Header";
import OverviewTab from "./components/tabs/OverviewTab";
import ProducersTab from "./components/tabs/ProducersTab";
import TabsNav from "./components/layout/TabsNav";
import TopicsTab from "./components/tabs/TopicsTab";
import WorkflowsTab from "./components/tabs/WorkflowsTab";
import { TABS } from "./constants/dashboard";
import { useDashboardData } from "./hooks/useDashboardData";
import { useState } from "react";

export default function App() {
  const [activeTab, setActiveTab] = useState("Overview");
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDLQ, setSelectedDLQ] = useState(null);

  const {
    consumers,
    dlqMessages,
    dlqTopic,
    error,
    events,
    group,
    health,
    loading,
    producerReasons,
    refresh,
    runs,
    setDlqTopic,
    setGroup,
    spark,
    tick,
    topics,
    totalConsumed,
    totalDLQ,
    totalInflight,
    totalProduced,
    totalRejected,
    updatedAt,
    version
  } = useDashboardData(activeTab);

  return (
    <div className="dq-root">
      <Header version={version} health={health} updatedAt={updatedAt} />

      <TabsNav tabs={TABS} activeTab={activeTab} onSelect={setActiveTab} />

      <main className="dq-main">
        {error ? <div className="dq-error">refresh error: {error}</div> : null}

        <Controls
          group={group}
          onGroupChange={setGroup}
          onGroupBlur={() => {
            if (!group.trim()) {
              setGroup("bench");
            }
          }}
          onRefresh={() => refresh(new AbortController().signal)}
          loading={loading}
          tick={tick}
        />

        {
          activeTab === "Overview" ? (
            <OverviewTab
              totalProduced={totalProduced}
              totalConsumed={totalConsumed}
              totalInflight={totalInflight}
              totalDLQ={totalDLQ}
              consumersCount={consumers.length}
              totalRejected={totalRejected}
              topics={topics}
              spark={spark}
              events={events}
            />
          ) : null
        }

        {
          activeTab === "Topics"
            ? <TopicsTab topics={topics} spark={spark} onTopicsChanged={() => refresh(new AbortController().signal)} />
            : null
        }

        {
          activeTab === "Producers" ? (
            <ProducersTab
              producerReasons={producerReasons}
              topics={topics}
              onProduced={() => refresh(new AbortController().signal)}
            />
          ) : null
        }

        {
          activeTab === "Consumers" ? (
            <ConsumersTab consumers={consumers} onConsumerChanged={() => refresh(new AbortController().signal)} />
          ) : null
        }

        {
          activeTab === "Dead Letters" ? (
            <DeadLettersTab
              dlqTopic={dlqTopic}
              onDlqTopicChange={setDlqTopic}
              dlqMessages={dlqMessages}
              selectedDLQ={selectedDLQ}
              onToggleInspect={(id) => setSelectedDLQ((prev) => (prev === id ? null : id))}
              topics={topics}
              onRedrive={() => refresh(new AbortController().signal)}
            />
          ) : null
        }

        {
          activeTab === "Workflows (v2)" ? (
            <WorkflowsTab runs={runs} selectedRun={selectedRun} onSelectRun={(id) => setSelectedRun((prev) => (prev === id ? null : id))} />
          ) : null
        }
      </main>

      <Footer tick={tick} />
    </div>
  );
}
