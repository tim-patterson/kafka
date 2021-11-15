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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

final class TaskMovement {
    private final TaskId task;
    private final UUID source;
    private final UUID destination;

    private TaskMovement(final TaskId task, final UUID source, final UUID destination) {
        this.task = task;
        this.source = source;
        this.destination = destination;
    }

    private static UUID moreCaughtUpClientIfTaskIsNotCaughtUp(final TaskId task,
                                                              final UUID clientId,
                                                              final Map<UUID, ClientState> clientStates,
                                                              final Map<TaskId, List<UUID>> tasksToClientsByLag,
                                                              final long acceptableRecoveryLag) {
        final List<UUID> clientsByLag = requireNonNull(tasksToClientsByLag.get(task), "uninitialized list");
        final UUID topCandidateId = clientsByLag.get(0);
        final ClientState topCandidate = clientStates.get(topCandidateId);
        final ClientState client = clientStates.get(clientId);
        final long topCandidateLag = Long.max(topCandidate.lagFor(task), 0);
        final long clientLag = Long.max(client.lagFor(task), 0);
        // Even if there is a more caught up client, as long as we're within allowable lag then
        // its best just to stick with what we've got
        if (clientLag > acceptableRecoveryLag && clientLag - topCandidateLag > acceptableRecoveryLag) {
            return topCandidateId;
        } else {
            return null;
        }
    }

    private static UUID moreCaughtUpClientIfTaskIsNotCaughtUpStandby(final TaskId task,
                                                                     final UUID clientId,
                                                                     final Map<UUID, ClientState> clientStates,
                                                                     final Map<TaskId, List<UUID>> tasksToClientsByLag,
                                                                     final long acceptableRecoveryLag) {
        final List<UUID> clientsByLag = requireNonNull(tasksToClientsByLag.get(task), "uninitialized list");
        final ClientState client = clientStates.get(clientId);
        final long clientLag = Long.max(client.lagFor(task), 0);
        for (final UUID candidateId : clientsByLag) {
            final ClientState candidate = clientStates.get(candidateId);
            if (!candidate.hasAssignedTask(task)) {
                final long topCandidateLag = Long.max(candidate.lagFor(task), 0);
                // Even if there is a more caught up client, as long as we're within allowable lag then
                // its best just to stick with what we've got
                if (clientLag > acceptableRecoveryLag && clientLag - topCandidateLag > acceptableRecoveryLag) {
                    return candidateId;
                }
            }
        }
        return null;
    }

    static int assignActiveTaskMovements(final Map<TaskId, List<UUID>> tasksToClientsByLag,
                                         final Map<UUID, ClientState> clientStates,
                                         final Map<UUID, Set<TaskId>> warmups,
                                         final AtomicInteger remainingWarmupReplicas,
                                         final long acceptableRecoveryLag) {
        final List<TaskMovement> taskMovements = new ArrayList<>();

        for (final Map.Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID client = clientStateEntry.getKey();
            final ClientState state = clientStateEntry.getValue();
            for (final TaskId task : state.activeTasks()) {
                // if the desired client is not caught up, and there is another client that _is_ more caught up,
                // then we schedule a movement, so we can move the active task to the caught-up client.
                // We'll try to assign a warm-up to the desired client so that we can move it later on.
                final UUID source = moreCaughtUpClientIfTaskIsNotCaughtUp(task,
                                                                          client,
                                                                          clientStates,
                                                                          tasksToClientsByLag,
                                                                          acceptableRecoveryLag);
                if (source != null) {
                    taskMovements.add(new TaskMovement(task, source, client));
                }
            }
        }

        final int movementsNeeded = taskMovements.size();

        for (final TaskMovement movement : taskMovements) {
            final UUID sourceId = movement.source;
            final ClientState source = clientStates.get(sourceId);

            if (!source.hasStandbyTask(movement.task)) {
                // there's not a caught-up standby available to take over the task, so we'll schedule a warmup instead
                moveActiveAndTryToWarmUp(
                    remainingWarmupReplicas,
                    movement.task,
                    source,
                    clientStates.get(movement.destination),
                    warmups.computeIfAbsent(movement.destination, x -> new TreeSet<>())
                );
            } else {
                // we found a candidate to trade standby/active state with our destination, so we don't need a warmup
                swapStandbyAndActive(
                    movement.task,
                    source,
                    clientStates.get(movement.destination)
                );
            }
        }

        return movementsNeeded;
    }

    static int assignStandbyTaskMovements(final Map<TaskId, List<UUID>> tasksToClientsByLag,
                                          final Map<UUID, ClientState> clientStates,
                                          final Map<UUID, Set<TaskId>> warmups,
                                          final AtomicInteger remainingWarmupReplicas,
                                          final long acceptableRecoveryLag) {

        final List<TaskMovement> taskMovements = new ArrayList<>();

        for (final Map.Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID destination = clientStateEntry.getKey();
            final ClientState state = clientStateEntry.getValue();
            for (final TaskId task : state.standbyTasks()) {
                if (warmups.getOrDefault(destination, Collections.emptySet()).contains(task)) {
                    // this is a warmup, so we won't move it.
                } else {
                    // if the desired client is not caught up, and there is another client that _is_ more caught up,
                    // then we schedule a movement, so we can move the standby task to the caught-up client.
                    // We'll try to assign a warm-up to the desired client so that we can move it later on.
                    final UUID source = moreCaughtUpClientIfTaskIsNotCaughtUpStandby(task,
                            destination,
                            clientStates,
                            tasksToClientsByLag,
                            acceptableRecoveryLag);
                    if (source != null) {
                        taskMovements.add(new TaskMovement(task, source, destination));
                    }
                }
            }
        }

        int movementsNeeded = 0;

        for (final TaskMovement movement : taskMovements) {
            final ClientState source = clientStates.get(movement.source);
            if (!source.hasAssignedTask(movement.task)) {
                moveStandbyAndTryToWarmUp(
                        remainingWarmupReplicas,
                        movement.task,
                        source,
                        clientStates.get(movement.destination)
                );
                movementsNeeded++;
            }
        }

        return movementsNeeded;
    }

    private static void moveActiveAndTryToWarmUp(final AtomicInteger remainingWarmupReplicas,
                                                 final TaskId task,
                                                 final ClientState sourceClientState,
                                                 final ClientState destinationClientState,
                                                 final Set<TaskId> warmups) {
        sourceClientState.assignActive(task);

        if (remainingWarmupReplicas.getAndDecrement() > 0) {
            destinationClientState.unassignActive(task);
            destinationClientState.assignStandby(task);
            warmups.add(task);
        } else {
            // we have no more standbys or warmups to hand out, so we have to try and move it
            // to the destination in a follow-on rebalance
            destinationClientState.unassignActive(task);
        }
    }

    private static void moveStandbyAndTryToWarmUp(final AtomicInteger remainingWarmupReplicas,
                                                  final TaskId task,
                                                  final ClientState sourceClientState,
                                                  final ClientState destinationClientState) {
        sourceClientState.assignStandby(task);

        if (remainingWarmupReplicas.getAndDecrement() > 0) {
            // Then we can leave it also assigned to the destination as a warmup
        } else {
            // we have no more warmups to hand out, so we have to try and move it
            // to the destination in a follow-on rebalance
            destinationClientState.unassignStandby(task);
        }
    }

    private static void swapStandbyAndActive(final TaskId task,
                                             final ClientState sourceClientState,
                                             final ClientState destinationClientState) {
        sourceClientState.unassignStandby(task);
        sourceClientState.assignActive(task);
        destinationClientState.unassignActive(task);
        destinationClientState.assignStandby(task);
    }

}
