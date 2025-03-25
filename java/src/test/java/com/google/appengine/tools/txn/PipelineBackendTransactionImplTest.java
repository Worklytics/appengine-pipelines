package com.google.appengine.tools.txn;

import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.cloud.datastore.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineBackendTransactionImplTest {

  private Transaction mockTransaction;
  private PipelineTaskQueue mockTaskQueue;
  private PipelineBackendTransactionImpl pipelineBackendTransaction;

  @BeforeEach
  void setUp() {
    mockTransaction = mock(Transaction.class);
    mockTaskQueue = mock(PipelineTaskQueue.class);
    pipelineBackendTransaction = PipelineBackendTransactionImpl.of(mockTransaction, mockTaskQueue);
  }

  @Test
  void commit() {
    when(mockTransaction.isActive()).thenReturn(true);
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenReturn(Collections.emptyList());

    pipelineBackendTransaction.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    pipelineBackendTransaction.commit();

    verify(mockTransaction).commit();
    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
  }

  @Test
  void commitQueueFailsDeletesTasks() {
    when(mockTransaction.isActive()).thenReturn(true);
    Set<PipelineTaskQueue.TaskReference> taskReferences = Collections.singleton(PipelineTaskQueue.TaskReference.of("queue1", "task-ref"));
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenThrow(new RuntimeException("error enqueueing"));

    pipelineBackendTransaction.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    assertThrows(RuntimeException.class, () -> pipelineBackendTransaction.commit());

    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
  }

  @Test
  void commitDatastoreFailsDeletesTasks() {
    when(mockTransaction.isActive()).thenReturn(true);
    List<PipelineTaskQueue.TaskReference> taskReferences = Collections.singletonList(PipelineTaskQueue.TaskReference.of("queue1", "task-ref"));
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenReturn(taskReferences);
    when(mockTransaction.commit()).thenThrow(new RuntimeException("error committing"));

    pipelineBackendTransaction.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());

    assertThrows(RuntimeException.class, () -> pipelineBackendTransaction.commit());

    verify(mockTransaction, atMostOnce()).commit();
    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
    verify(mockTaskQueue).deleteTasks(ArgumentMatchers.argThat(taskReferences::containsAll));
  }

  @Test
  void addTask() {
    PipelineTaskQueue.TaskSpec task = PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build();
    pipelineBackendTransaction.addTask("queue1", task);

    assertFalse(pipelineBackendTransaction.getTasksByQueue().isEmpty());
    assertTrue(pipelineBackendTransaction.getTasksByQueue().containsEntry("queue1", task));
  }

  @Test
  void rollback() {
    pipelineBackendTransaction.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    pipelineBackendTransaction.rollback();

    verify(mockTransaction).rollback();
    assertTrue(pipelineBackendTransaction.getTasksByQueue().isEmpty());
  }

  @Test
  void rollbackIfActive() {
    when(mockTransaction.isActive()).thenReturn(true);

    pipelineBackendTransaction.rollbackIfActive();

    verify(mockTransaction).rollback();
  }

  @Test
  void of() {
    PipelineBackendTransactionImpl pipelineBackendTransaction = PipelineBackendTransactionImpl.of(mockTransaction, mockTaskQueue);
    assertNotNull(pipelineBackendTransaction);
    assertEquals(mockTransaction, pipelineBackendTransaction.getDsTransaction());
  }

  @Test
  void getDsTransaction() {
    assertEquals(mockTransaction, pipelineBackendTransaction.getDsTransaction());
  }
}