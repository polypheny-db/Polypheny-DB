/*
 * Copyright 2019-2021 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring.core;

import java.sql.Timestamp;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.MonitoringMetric;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;
import org.polypheny.db.monitoring.subscriber.MonitoringMetricSubscriber;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;

@Slf4j
public class MonitoringServiceImpl implements MonitoringService {

    // region private fields

    private final MonitoringQueue monitoringQueue;
    private final MonitoringRepository repository;
    private final MonitoringServiceUi monitoringServiceUi;

    // endregion

    // region ctors


    public MonitoringServiceImpl(
            @NonNull MonitoringQueue monitoringQueue,
            @NonNull MonitoringRepository repository,
            @NonNull MonitoringServiceUi monitoringServiceUi ) {

        this.monitoringQueue = monitoringQueue;
        this.repository = repository;
        this.monitoringServiceUi = monitoringServiceUi;
    }

    // endregion

    // region public methods


    @Override
    public void monitorEvent( @NonNull MonitoringEvent eventData ) {
        this.monitoringQueue.queueEvent( eventData );
    }


    @Override
    public <T extends MonitoringMetric> void subscribeMetric( @NonNull Class<T> eventDataClass, @NonNull MonitoringMetricSubscriber<T> subscriber ) {
        this.monitoringQueue.subscribeMetric( eventDataClass, subscriber );
    }


    @Override
    public <T extends MonitoringMetric> void unsubscribeMetric( @NonNull Class<T> eventDataClass, @NonNull MonitoringMetricSubscriber<T> subscriber ) {
        this.monitoringQueue.unsubscribeMetric( eventDataClass, subscriber );
    }


    @Override
    public void unsubscribeFromAllMetrics( @NonNull MonitoringMetricSubscriber subscriber ) {
        this.monitoringQueue.unsubscribeFromAllMetrics( subscriber );
    }


    @Override
    public <T extends MonitoringMetric> List<T> getAllMetrics( @NonNull Class<T> metricClass ) {
        return this.repository.getAllMetrics( metricClass );
    }


    @Override
    public <T extends MonitoringMetric> List<T> getMetricsBefore( @NonNull Class<T> metricClass, @NonNull Timestamp timestamp ) {
        return this.repository.getMetricsBefore( metricClass, timestamp );
    }


    @Override
    public <T extends MonitoringMetric> List<T> getMetricsAfter( @NonNull Class<T> metricClass, @NonNull Timestamp timestamp ) {
        return this.repository.getMetricsAfter( metricClass, timestamp );
    }

    // endregion
}
