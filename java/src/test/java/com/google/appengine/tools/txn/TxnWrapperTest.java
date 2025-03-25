package com.google.appengine.tools.txn;

import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.cloud.datastore.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TxnWrapperTest {

  private Transaction mockTransaction;
  private PipelineTaskQueue mockTaskQueue;
  private TxnWrapper txnWrapper;

  @BeforeEach
  void setUp() {
    mockTransaction = mock(Transaction.class);
    mockTaskQueue = mock(PipelineTaskQueue.class);
    txnWrapper = TxnWrapper.of(mockTransaction, mockTaskQueue);
  }

  @Test
  void commit() {
    when(mockTransaction.isActive()).thenReturn(true);
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenReturn(Collections.emptyList());

    txnWrapper.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    txnWrapper.commit();

    verify(mockTransaction).commit();
    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
  }

  @Test
  void commitQueueFailsDeletesTasks() {
    when(mockTransaction.isActive()).thenReturn(true);
    Set<PipelineTaskQueue.TaskReference> taskReferences = Collections.singleton(PipelineTaskQueue.TaskReference.of("queue1", "task-ref"));
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenThrow(new RuntimeException("error enqueueing"));

    txnWrapper.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    assertThrows(RuntimeException.class, () -> txnWrapper.commit());

    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
  }

  @Test
  void commitDatastoreFailsDeletesTasks() {
    when(mockTransaction.isActive()).thenReturn(true);
    List<PipelineTaskQueue.TaskReference> taskReferences = Collections.singletonList(PipelineTaskQueue.TaskReference.of("queue1", "task-ref"));
    when(mockTaskQueue.enqueue(anyString(), anyCollection())).thenReturn(taskReferences);
    when(mockTransaction.commit()).thenThrow(new RuntimeException("error committing"));

    txnWrapper.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());

    assertThrows(RuntimeException.class, () -> txnWrapper.commit());

    verify(mockTransaction, atMostOnce()).commit();
    verify(mockTaskQueue).enqueue(anyString(), anyCollection());
    verify(mockTaskQueue).deleteTasks(ArgumentMatchers.argThat(taskReferences::containsAll));
  }

  @Test
  void addTask() {
    PipelineTaskQueue.TaskSpec task = PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build();
    txnWrapper.addTask("queue1", task);

    assertFalse(txnWrapper.getTasksByQueue().isEmpty());
    assertTrue(txnWrapper.getTasksByQueue().containsEntry("queue1", task));
  }

  @Test
  void rollback() {
    txnWrapper.addTask("queue1", PipelineTaskQueue.TaskSpec.builder().host("any").method(PipelineTaskQueue.TaskSpec.Method.GET).callbackPath("path").build());
    txnWrapper.rollback();

    verify(mockTransaction).rollback();
    assertTrue(txnWrapper.getTasksByQueue().isEmpty());
  }

  @Test
  void rollbackIfActive() {
    when(mockTransaction.isActive()).thenReturn(true);

    txnWrapper.rollbackIfActive();

    verify(mockTransaction).rollback();
  }

  @Test
  void of() {
    TxnWrapper txnWrapper = TxnWrapper.of(mockTransaction, mockTaskQueue);
    assertNotNull(txnWrapper);
    assertEquals(mockTransaction, txnWrapper.getDsTransaction());
  }

  @Test
  void getDsTransaction() {
    assertEquals(mockTransaction, txnWrapper.getDsTransaction());
  }
}