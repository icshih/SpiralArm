/*
 * CU9 WP940 Validation
 * Copyright (C) 2006-2014 Gaia Data Processing and Analysis Consortium
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */
package ics.astro.tap;

/**
 * Name of available TAP Services
 *
 * @author I-Chun Shih
 * @version $Id: TapService.java 529400 2016-09-23 09:13:05Z nleclerc $
 */
public enum TapService {
    TEST("https://gea.esac.esa.int"),
    GACS("http://gea.esac.esa.int"),
    SECUREDGACS("https://gea.esac.esa.int"),
    GEPI("http://cu9tap.obspm.fr"),
    HEIDELBERG("http://dc.zah.uni-heidelberg.de"),
    VIZIER("http://TAPVizieR.u-strasbg.fr/TAPVizieR/tap/");

    private final String url;

    /**
     *
     * @param URL
     */
    TapService(String URL) {
        this.url = URL;
    }

    /**
     * @return the URL associated to a given TAP service
     */
    public String getTAPURL() {
        return url;
    }
}
