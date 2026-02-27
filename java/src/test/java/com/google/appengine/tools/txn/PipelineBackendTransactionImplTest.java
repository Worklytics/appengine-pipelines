package com.google.appengine.tools.txn;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Transaction;

class PipelineBackendTransactionImplTest {

  private Datastore mockDatastore;
  private Transaction mockTransaction;
  private PipelineTaskQueue mockTaskQueue;
  private PipelineBackendTransactionImpl pipelineBackendTransaction;

  @BeforeEach
  void setUp() {
    mockTransaction = mock(Transaction.class);
    mockTaskQueue = mock(PipelineTaskQueue.class);
    mockDatastore = mock(Datastore.class);
    when(mockDatastore.newTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.getTransactionId()).thenReturn(com.google.protobuf.ByteString.copyFromUtf8("mock-txn-id"));
    pipelineBackendTransaction = new PipelineBackendTransactionImpl(mockDatastore, mockTaskQueue);
  }

  @Test
  void commit() {
    when(mockTransaction.isActive()).thenReturn(true);
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenReturn(Collections.emptyList());

    pipelineBackendTransaction.enqueue("queue1", PipelineTaskQueue.TaskSpec.builder()
        .method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    pipelineBackendTransaction.commit();

    verify(mockTransaction).commit();
    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
  }

  @Test
  void commitQueueFailsDeletesTasks() {
    when(mockTransaction.isActive()).thenReturn(true);
    Set<PipelineTaskQueue.TaskReference> taskReferences = Collections
        .singleton(PipelineTaskQueue.TaskReference.of("queue1", "task-ref"));
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenThrow(new RuntimeException("error enqueueing"));

    pipelineBackendTransaction.enqueue("queue1", PipelineTaskQueue.TaskSpec.builder()
        .method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    assertThrows(RuntimeException.class, () -> pipelineBackendTransaction.commit());

    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
  }

  @Test
  void commitDatastoreFailsDeletesTasks() {
    when(mockTransaction.isActive()).thenReturn(true);
    List<PipelineTaskQueue.TaskReference> taskReferences = Collections
        .singletonList(PipelineTaskQueue.TaskReference.of("queue1", "task-ref"));
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenReturn(taskReferences);
    when(mockTransaction.commit()).thenThrow(new RuntimeException("error committing"));

    pipelineBackendTransaction.enqueue("queue1", PipelineTaskQueue.TaskSpec.builder()
        .method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());

    assertThrows(RuntimeException.class, () -> pipelineBackendTransaction.commit());

    verify(mockTransaction, atMostOnce()).commit();
    verify(mockTaskQueue, org.mockito.Mockito.never()).enqueue(anyString(), anyCollection());
    verify(mockTaskQueue, org.mockito.Mockito.never()).deleteTasks(ArgumentMatchers.any());
  }

  @Test
  void enqueue() {
    PipelineTaskQueue.TaskSpec task = PipelineTaskQueue.TaskSpec.builder().method(PipelineTaskQueue.TaskSpec.Method.GET)
        .callbackPath("path").build();
    pipelineBackendTransaction.enqueue("queue1", task);

    assertFalse(pipelineBackendTransaction.getPendingTaskSpecsByQueue().isEmpty());
    assertTrue(pipelineBackendTransaction.getPendingTaskSpecsByQueue().containsEntry("queue1", task));
  }

  @Test
  void rollback() {
    pipelineBackendTransaction.enqueue("queue1", PipelineTaskQueue.TaskSpec.builder()
        .method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    pipelineBackendTransaction.rollback();

    verify(mockTransaction).rollback();
    assertTrue(pipelineBackendTransaction.getPendingTaskSpecsByQueue().isEmpty());
  }

  @Test
  void rollbackIfActive() {
    when(mockTransaction.isActive()).thenReturn(true);

    pipelineBackendTransaction.rollbackIfActive();

    verify(mockTransaction).rollback();
  }

}