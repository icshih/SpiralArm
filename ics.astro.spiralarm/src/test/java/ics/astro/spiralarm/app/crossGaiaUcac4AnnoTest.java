package ics.astro.spiralarm.app;

import ics.astro.spiralarm.dm.crossGaiaUcac4Dm;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.service.ServiceRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class crossGaiaUcac4AnnoTest {

    private SessionFactory sessionFactory;

    @BeforeEach
    void setUp() {
        StandardServiceRegistry standardRegistry = new StandardServiceRegistryBuilder()
                .configure( "hibernate.cfg.xml" )
                .build();
        sessionFactory = new MetadataSources(standardRegistry).buildMetadata().buildSessionFactory();
    }

    @AfterEach
    void tearDown() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
    }

    @Test
    void testGetSelect() {
        System.out.print(crossGaiaUcac4.getSelect("g"));
    }

    @Test
    void testBasicUsage() {
        // create a couple of events...
        assertNotNull(sessionFactory);
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        session.save( new crossGaiaUcac4Dm( ) );
        session.save( new crossGaiaUcac4Dm( ) );
        session.getTransaction().commit();
        session.close();

        // now lets pull events from the database and list them
        session = sessionFactory.openSession();
        session.beginTransaction();
        List result = session.createQuery( String.format("from %s", crossGaiaUcac4.crossTableName) ).list();
        for ( crossGaiaUcac4Dm event : (List<crossGaiaUcac4Dm>) result ) {
            System.out.println( "Data (" + event.getSourceId() + ") : " + event.getUcac4Id() );
        }
        session.getTransaction().commit();
        session.close();
    }
}
