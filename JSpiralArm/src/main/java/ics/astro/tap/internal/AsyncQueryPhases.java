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
package ics.astro.tap.internal;

/**
 * Enumerations of the phases of asynchronous query of Table Access Protocol
 * (TAP)
 *
 * @author I-Chun Shih (i-chun.shih at obspm.fr)
 * @version $Id: AsyncQueryPhases.java 529400 2016-09-23 09:13:05Z nleclerc $
 */
public enum AsyncQueryPhases {
    RUN, QUEUED, EXECUTING, COMPLETED, ABORTED, ERROR
}
