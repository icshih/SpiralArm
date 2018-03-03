module ics.astro.spiralarm {
    requires java.naming;
    requires javax.transaction.api;
    requires slf4j.api;
    requires logback.classic;
    requires hibernate.core;
    requires hibernate.jpa;
    requires ics.tap;
    requires stil;
    exports ics.astro.spiralarm.app;
    exports ics.astro.spiralarm.dm;
}