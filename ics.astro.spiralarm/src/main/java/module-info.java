module ics.astro.spiralarm {
    requires slf4j.api;
    requires logback.classic;
    requires hibernate.jpa;
    requires ics.tap;
    exports ics.astro.spiralarm.dm;
}