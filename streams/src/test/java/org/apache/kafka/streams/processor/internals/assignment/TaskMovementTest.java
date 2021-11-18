/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.hasProperty;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignActiveTaskMovements;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TaskMovementTest {
    @Test
    public void shouldAssignTasksToClientsAndReturnFalseWhenAllClientsCaughtUp() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<TaskId, List<UUID>> tasksToClientsByLag = new HashMap<>();
        for (final TaskId task : allTasks) {
            tasksToClientsByLag.put(task, asList(UUID_1, UUID_2, UUID_3));
        }

        final ClientState client1 = getClientStateWithActiveAssignment(mkSet(TASK_0_0, TASK_1_0), emptySet(), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(mkSet(TASK_0_1, TASK_1_1), emptySet(), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(mkSet(TASK_0_2, TASK_1_2), emptySet(), allTasks);

        assertThat(
            assignActiveTaskMovements(
                tasksToClientsByLag,
                getClientStatesMap(client1, client2, client3),
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas),
                100L),
            is(0)
        );
    }

    @Test
    public void shouldAssignAllTasksToClientsAndReturnFalseIfNoClientsAreCaughtUp() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<TaskId, List<UUID>> tasksToClientsByLag = new HashMap<>();
        for (final TaskId task : allTasks) {
            tasksToClientsByLag.put(task, asList(UUID_1, UUID_2, UUID_3));
        }

        final ClientState client1 = getClientStateWithActiveAssignment(mkSet(TASK_0_0, TASK_1_0), emptySet(), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(mkSet(TASK_0_1, TASK_1_1), emptySet(), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(mkSet(TASK_0_2, TASK_1_2), emptySet(), allTasks);

        assertThat(
            assignActiveTaskMovements(
                tasksToClientsByLag,
                getClientStatesMap(client1, client2, client3),
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas),
                100L),
            is(0)
        );
    }

    @Test
    public void shouldMoveTasksToCaughtUpClientsAndAssignWarmupReplicasInTheirPlace() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState client1 = getClientStateWithActiveAssignment(mkSet(TASK_0_0), mkSet(TASK_0_0), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(mkSet(TASK_0_1), mkSet(TASK_0_2), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(mkSet(TASK_0_2), mkSet(TASK_0_1), allTasks);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, List<UUID>> tasksToClientsByLag = mkMap(
            mkEntry(TASK_0_0, asList(UUID_1, UUID_2, UUID_3)),
            mkEntry(TASK_0_1, asList(UUID_3, UUID_1, UUID_2)),
            mkEntry(TASK_0_2, asList(UUID_2, UUID_1, UUID_3))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToClientsByLag,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas),
                100L),
            is(2)
        );
        // The active tasks have changed to the ones that each client is caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_1)));

        // we assigned warmups to migrate to the input active assignment
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_1)));
        assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_2)));
    }

    @Test
    public void shouldOnlyGetUpToMaxWarmupReplicasAndReturnTrue() {
        final int maxWarmupReplicas = 1;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState client1 = getClientStateWithActiveAssignment(mkSet(TASK_0_0), mkSet(TASK_0_0), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(mkSet(TASK_0_1), mkSet(TASK_0_2), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(mkSet(TASK_0_2), mkSet(TASK_0_1), allTasks);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, List<UUID>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, asList(UUID_1, UUID_2, UUID_3)),
            mkEntry(TASK_0_1, asList(UUID_3, UUID_1, UUID_2)),
            mkEntry(TASK_0_2, asList(UUID_2, UUID_1, UUID_3))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas),
                100L),
            is(2)
        );
        // The active tasks have changed to the ones that each client is caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_1)));

        // we should only assign one warmup, but it could be either one that needs to be migrated.
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        try {
            assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_1)));
            assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        } catch (final AssertionError ignored) {
            assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
            assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_2)));
        }
    }

    @Test
    public void shouldNotCountPreviousStandbyTasksTowardsMaxWarmupReplicas() {
        final int maxWarmupReplicas = 0;
        final Set<TaskId> allTasks = mkSet(TASK_0_0);
        final ClientState client1 = getClientStateWithActiveAssignment(emptySet(), mkSet(TASK_0_0), allTasks);
        client1.assignStandby(TASK_0_0);
        final ClientState client2 = getClientStateWithActiveAssignment(mkSet(TASK_0_0), emptySet(), allTasks);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final Map<TaskId, List<UUID>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, asList(UUID_1, UUID_2))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas),
                100L),
            is(1)
        );
        // Even though we have no warmups allowed, we still let client1 take over active processing while
        // client2 "warms up" because client1 was a caught-up standby, so it can "trade" standby status with
        // the not-caught-up active client2.

        // I.e., when you have a caught-up standby and a not-caught-up active, you can just swap their roles
        // and not call it a "warmup".
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, mkSet()));

        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_0)));

    }

    private static ClientState getClientStateWithActiveAssignment(final Set<TaskId> activeTasks,
                                                                  final Set<TaskId> caughtUpTasks,
                                                                  final Set<TaskId> allTasks) {
        final Map<TaskId, Long> lags = new HashMap<>();
        for (final TaskId task : allTasks) {
            if (caughtUpTasks.contains(task)) {
                lags.put(task, 0L);
            } else {
                lags.put(task, Long.MAX_VALUE);
            }
        }
        final ClientState client1 = new ClientState(activeTasks, emptySet(), lags, 10);
        client1.assignActiveTasks(activeTasks);
        return client1;
    }

}
