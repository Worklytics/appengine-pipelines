// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.examples;


import com.google.appengine.tools.pipeline.*;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author rudominer@google.com (Mitch Rudominer)
 *
 */
public class AsyncGCDExample {

  /**
   * A Callback
   */
  public interface Callback {
    int getFirstInt();

    int getSecondInt();

    String getUserName();

    void acceptOutput(String output);
  }

  public static volatile Callback callback;

  /**
   * 1. Start a new thread in which we ask the user for two integers. 2. After
   * the user responds, calculate the GCD of the two integers 3. Start a new
   * thread in which we ask the user for his name. 4. After the user responds,
   * print a message on the console with the results.
   */
  @SuppressWarnings("serial")
  @RequiredArgsConstructor
  public static class PrintGCDJob extends Job0<Void> {
    @Override
    public Value<Void> run() {

      PromisedValue<Integer> a = newPromise();
      PromisedValue<Integer> b = newPromise();
      asyncAskUserForTwoIntegers(a.getHandle(), b.getHandle());
      FutureValue<Integer> gcd = futureCall(new GCDExample.GCDJob(), a, b);
      // Don't ask the user for his name until after he has already
      // answered the first prompt asking for two integers.
      FutureValue<String> userName = futureCall(new AskUserForNameJob(), waitFor(b));
      futureCall(new PrintResultJob(), userName, a, b, gcd);
      return null;
    }

    private void asyncAskUserForTwoIntegers(final SlotId aHandle, final SlotId bHandle) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          int a = callback.getFirstInt();
          int b = callback.getSecondInt();
          try {
            getPipelineService().submitPromisedValue(aHandle, a);
            getPipelineService().submitPromisedValue(bHandle, b);
          } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
          } catch (OrphanedObjectException f) {
            return;
          }
        }
      };
      thread.start();
    }
  }

  /**
   * Prints a prompt on the console asking the user for his name, and then
   * starts a new thread which waits for the user to enter his name.
   */
  @SuppressWarnings("serial")
  @RequiredArgsConstructor
  public static class AskUserForNameJob extends Job0<String> {

    @Override
    public Value<String> run() {

      PromisedValue<String> userName = newPromise();
      asyncAskUserForName(userName.getHandle());
      return userName;
    }

    private void asyncAskUserForName(final SlotId handle) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          String name = callback.getUserName();
          try {
            getPipelineService().submitPromisedValue(handle, name);
          } catch (NoSuchObjectException e) {
            throw new RuntimeException(e);
          } catch (OrphanedObjectException f) {
            return;
          }
        }
      };
      thread.start();
    }
  }

  /**
   * Prints result message to console
   */
  @SuppressWarnings("serial")
  public static class PrintResultJob extends Job4<Void, String, Integer, Integer, Integer> {
    @Override
    public Value<Void> run(String userName, Integer a, Integer b, Integer gcd) {
      String output =
          "Hello, " + userName + ". The GCD of " + a + " and " + b + " is " + gcd + ".";
      callback.acceptOutput(output);
      return null;
    }
  }
}
