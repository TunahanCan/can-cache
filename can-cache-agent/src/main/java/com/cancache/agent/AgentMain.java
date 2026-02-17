package com.cancache.agent;

import com.cancache.agent.dashboard.TuiDashboard;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@QuarkusMain
public class AgentMain implements QuarkusApplication
{
    private static final Logger LOG = Logger.getLogger(AgentMain.class);

    @Inject
    TuiDashboard dashboard;

    static void main(String[] args) {
        Quarkus.run(AgentMain.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        LOG.info("Can-Cache-Agent starting");
        dashboard.start();
        Quarkus.waitForExit();
        return 0;
    }

    void onStop(@Observes ShutdownEvent event) {
        LOG.info("Can-Cache-Agent stopping");
        dashboard.stop();
    }
}
