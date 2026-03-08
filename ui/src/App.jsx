import ConsumersTab from "./features/consumers/components/ConsumersTab";
import Controls from "./features/layout/components/Controls";
import DeadLettersTab from "./features/deadLetters/components/DeadLettersTab";
import Footer from "./features/layout/components/Footer";
import Header from "./features/layout/components/Header";
import MessagesTab from "./features/messages/components/MessagesTab";
import OverviewTab from "./features/overview/components/OverviewTab";
import ProducersTab from "./features/producers/components/ProducersTab";
import TabsNav from "./features/layout/components/TabsNav";
import TopicsTab from "./features/topics/components/TopicsTab";
import WorkflowsTab from "./features/workflows/components/WorkflowsTab";
import { TABS } from "./constants/dashboard";
import { useDashboardData } from "./features/dashboard/hooks/useDashboardData";
import { useState } from "react";

export default function App() {
  const [activeTab, setActiveTab] = useState("Overview");
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDLQ, setSelectedDLQ] = useState(null);
  const [pendingConsumerMessage, setPendingConsumerMessage] = useState(null);

  const {
    consumers,
    config,
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
      <Header version={version} health={health} updatedAt={updatedAt} config={config} />

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
              config={config}
              version={version}
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
            <ConsumersTab
              consumers={consumers}
              pendingMessage={pendingConsumerMessage}
              onPendingMessageChange={setPendingConsumerMessage}
              onConsumerChanged={() => refresh(new AbortController().signal)}
            />
          ) : null
        }

        {
          activeTab === "Messages" ? (
            <MessagesTab
              group={group}
              topics={topics}
            />
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
            <WorkflowsTab
              runs={runs}
              selectedRun={selectedRun}
              onSelectRun={(id, options = {}) => {
                setSelectedRun((prev) => {
                  if (options.toggle === false) {
                    return id;
                  }

                  return prev === id ? null : id;
                });
              }}
              onRunChanged={() => refresh(new AbortController().signal)}
            />
          ) : null
        }
      </main>

      <Footer tick={tick} />
    </div>
  );
}
