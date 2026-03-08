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
import { APP_COPY, APP_TAB, DEFAULTS, TABS } from "./constants/ui";
import { useDashboardData } from "./features/dashboard/hooks/useDashboardData";
import { useState } from "react";

export default function App() {
  const [activeTab, setActiveTab] = useState(APP_TAB.OVERVIEW);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDLQ, setSelectedDLQ] = useState(null);

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
        {error ? <div className="dq-error">{APP_COPY.REFRESH_ERROR_PREFIX} {error}</div> : null}

        <Controls
          group={group}
          onGroupChange={setGroup}
          onGroupBlur={() => {
            if (!group.trim()) {
              setGroup(DEFAULTS.GROUP);
            }
          }}
          onRefresh={() => refresh(new AbortController().signal)}
          loading={loading}
          tick={tick}
        />

        {
          activeTab === APP_TAB.OVERVIEW ? (
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
          activeTab === APP_TAB.TOPICS
            ? <TopicsTab topics={topics} spark={spark} onTopicsChanged={() => refresh(new AbortController().signal)} />
            : null
        }

        {
          activeTab === APP_TAB.PRODUCERS ? (
            <ProducersTab
              producerReasons={producerReasons}
              topics={topics}
              onProduced={() => refresh(new AbortController().signal)}
            />
          ) : null
        }

        {
          activeTab === APP_TAB.CONSUMERS ? (
            <ConsumersTab
              consumers={consumers}
              onConsumerChanged={() => refresh(new AbortController().signal)}
            />
          ) : null
        }

        {
          activeTab === APP_TAB.MESSAGES ? (
            <MessagesTab
              group={group}
              topics={topics}
            />
          ) : null
        }

        {
          activeTab === APP_TAB.DEAD_LETTERS ? (
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
          activeTab === APP_TAB.WORKFLOWS ? (
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
