/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.rm;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;

import org.apache.tez.dag.app.rm.UtilizationTable;
import org.apache.tez.dag.app.rm.UtilizationTable.Tuple;
import org.apache.tez.dag.app.rm.UtilizationRecord;
import org.apache.tez.dag.app.rm.UtilizationRecord.JobType;
import org.apache.tez.dag.app.rm.UtilizationRecord.UtilizationRecordType;

public class TestUtilizationTable {

  private static final int NUM_ITERATIONS = 10000;
  // Apparently this the random generator is pretty bad
  private static final double RANDOM_EPSILON = 0.02;
  // An actual epsilon for floating-point value comparison
  private static final double EPSILON = 0.0001;

  private static UtilizationRecord[] getUtilizationRecordsForTest() {
    UtilizationRecord[] records = new UtilizationRecord[7];
    // Periodic
    records[0] = new UtilizationRecord(
        UtilizationRecordType.U_PERIODIC,
        new HashSet<String>(Arrays.asList("a")),
        100.0, 200.0, 300.0, 100);
    records[5] = new UtilizationRecord(
        UtilizationRecordType.U_PERIODIC,
        new HashSet<String>(Arrays.asList("f")),
        300.0, 200.0, 400.0, 500);
    // Constant
    records[1] = new UtilizationRecord(
        UtilizationRecordType.U_CONSTANT,
        new HashSet<String>(Arrays.asList("b")),
        150.0, 150.0, 150.0, 200);
    records[2] = new UtilizationRecord(
        UtilizationRecordType.U_CONSTANT,
        new HashSet<String>(Arrays.asList("c")),
        350.0, 350.0, 350.0, 600);
    // Unpredictable
    records[3] = new UtilizationRecord(
        UtilizationRecordType.U_UNPREDICTABLE,
        new HashSet<String>(Arrays.asList("d")),
        300.0, 100.0, 100.0, 1000);
    records[4] = new UtilizationRecord(
        UtilizationRecordType.U_UNPREDICTABLE,
        new HashSet<String>(Arrays.asList("e")),
        220.0, 300.0, 440.0, 500);
    records[6] = new UtilizationRecord(
        UtilizationRecordType.U_UNPREDICTABLE,
        new HashSet<String>(Arrays.asList("g")),
        100.0, 170.0, 420.0, 600);

    return records;
  }

  @Test
  public void shortJobFitsSingleClassDeterministic() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    // start with probabilistic type selection disabled, and best-fit disabled
    UtilizationTable table = new UtilizationTable(false, 1.0, 1.0, 1.0, false,
                                                  null);
    table.setUtilizationRecords(records);
    // job can fits in a single class
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("d", "e"));
    double expected_prob_d = 300.0 / (300.0 + 220.0);
    double expected_prob_e = 220.0 / (300.0 + 220.0);
    double actual_prob_d = 0.0;
    double actual_prob_e = 0.0;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(200, 1, 2, JobType.T_JOB_SHORT);
      // only pick one class
      Assert.assertEquals(1, schedule.size());
      double probability = schedule.get(0).getFirst();
      HashSet<String> labels = schedule.get(0).getSecond();
      // upper bound of the CDF is 1.0
      Assert.assertEquals(1.0, probability, 0.0);
      // the label is legit
      Assert.assertTrue(possibleLabels.containsAll(labels));

      if (labels.contains("d")) {
        actual_prob_d += 1.0 / NUM_ITERATIONS;
      } else {
        actual_prob_e += 1.0 / NUM_ITERATIONS;
      }
    }

    // check the probability
    Assert.assertEquals(expected_prob_d, actual_prob_d, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_e, actual_prob_e, RANDOM_EPSILON);
  }

  @Test
  public void shortJobFitsSingleClassDeterministicBestFit() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    // start with probabilistic type selection disabled, and best-fit enabled
    UtilizationTable table = new UtilizationTable(false, 1.0, 1.0, 1.0, true,
                                                  null);
    table.setUtilizationRecords(records);
    // job can fits in a single class
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("d", "e"));
    double expected_prob_d = 1.0 / (1.0 + 81.0);
    double expected_prob_e = 81.0 / (1.0 + 81.0);
    double actual_prob_d = 0.0;
    double actual_prob_e = 0.0;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(200, 1, 2, JobType.T_JOB_SHORT);
      // only pick one class
      Assert.assertEquals(1, schedule.size());
      double probability = schedule.get(0).getFirst();
      HashSet<String> labels = schedule.get(0).getSecond();
      // upper bound of the CDF is 1.0
      Assert.assertEquals(1.0, probability, EPSILON);
      // the label is legit
      Assert.assertTrue(possibleLabels.containsAll(labels));

      if (labels.contains("d")) {
        actual_prob_d += 1.0 / NUM_ITERATIONS;
      } else {
        actual_prob_e += 1.0 / NUM_ITERATIONS;
      }
    }

    // check the probability
    Assert.assertEquals(expected_prob_d, actual_prob_d, RANDOM_EPSILON);
  }

  @Test
  public void shortJobFitsSingleClassProbabilistic() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    double highWeight = 4.0;
    double mediumWeight = 2.0;
    double lowWeight = 1.0;

    // try probabilistic type selection enabled, and best-fit disabled
    UtilizationTable table = new UtilizationTable(true, lowWeight,
                                                  mediumWeight,
                                                  highWeight, false, null);
    table.setUtilizationRecords(records);

    // set the counters
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("d", "e", "f", "c"));
    double sum = 300.0 * highWeight + 220.0 * highWeight +
        250.0 * mediumWeight + 300.0 * lowWeight;
    double expected_prob_d = (300.0 * highWeight) / sum;
    double expected_prob_e = (220.0 * highWeight) / sum;
    double expected_prob_f = (250.0 * mediumWeight) / sum;
    double expected_prob_c = (300.0 * lowWeight) / sum;
    double actual_prob_d = 0.0;
    double actual_prob_e = 0.0;
    double actual_prob_f = 0.0;
    double actual_prob_c = 0.0;

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(200, 1, 2, JobType.T_JOB_SHORT);
      // only pick one class
      Assert.assertEquals(1, schedule.size());
      double probability = schedule.get(0).getFirst();
      HashSet<String> labels = schedule.get(0).getSecond();
      // upper bound of the CDF is 1.0
      Assert.assertEquals(1.0, probability, EPSILON);
      // the label is legit
      Assert.assertTrue(possibleLabels.containsAll(labels));

      if (labels.contains("d")) {
        actual_prob_d += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("e")) {
        actual_prob_e += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("f")) {
        actual_prob_f += 1.0 / NUM_ITERATIONS;
      } else {
        actual_prob_c += 1.0 / NUM_ITERATIONS;
      }
    }

    // check the probability
    Assert.assertEquals(expected_prob_d, actual_prob_d, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_e, actual_prob_e, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_f, actual_prob_f, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_c, actual_prob_c, RANDOM_EPSILON);
  }

  @Test
  public void shortJobFitsSingleClassProbabilisticBestFit() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    double highWeight = 4.0;
    double mediumWeight = 2.0;
    double lowWeight = 1.0;

    // try probabilistic type selection enabled, and best-fit enabled 
    UtilizationTable table = new UtilizationTable(true, lowWeight,
                                                  mediumWeight,
                                                  highWeight, true, null);
    table.setUtilizationRecords(records);

    // set the counters
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("d", "e", "f", "c"));
    double sum = 1.0 * highWeight + 81.0 * highWeight +
        51.0 * mediumWeight + 1.0 * lowWeight;
    double expected_prob_d = (1.0 * highWeight) / sum;
    double expected_prob_e = (81.0 * highWeight) / sum;
    double expected_prob_f = (51.0 * mediumWeight) / sum;
    double expected_prob_c = (1.0 * lowWeight) / sum;
    double actual_prob_d = 0.0;
    double actual_prob_e = 0.0;
    double actual_prob_f = 0.0;
    double actual_prob_c = 0.0;

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(200, 1, 2, JobType.T_JOB_SHORT);
      // only pick one class
      Assert.assertEquals(1, schedule.size());
      double probability = schedule.get(0).getFirst();
      HashSet<String> labels = schedule.get(0).getSecond();
      // upper bound of the CDF is 1.0
      Assert.assertEquals(1.0, probability, EPSILON);
      // the label is legit
      Assert.assertTrue(possibleLabels.containsAll(labels));

      if (labels.contains("d")) {
        actual_prob_d += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("e")) {
        actual_prob_e += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("f")) {
        actual_prob_f += 1.0 / NUM_ITERATIONS;
      } else {
        actual_prob_c += 1.0 / NUM_ITERATIONS;
      }
    }

    // check the probability
    Assert.assertEquals(expected_prob_d, actual_prob_d, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_e, actual_prob_e, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_f, actual_prob_f, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_c, actual_prob_c, RANDOM_EPSILON);
  }

  @Test
  public void shortJobFitsMultipleClassesDeterministic() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    // start with probabilistic type selection disabled, and best-fit disabled
    UtilizationTable table = new UtilizationTable(false, 1.0, 1.0, 1.0, false,
                                                  null);
    table.setUtilizationRecords(records);
    // job can fits in a single class
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("d", "e", "g", "a", "f"));
    double expected_prob_d = 300.0 / 600.0;
    double expected_prob_e = 220.0 / 600.0;
    double expected_prob_g = 100.0 / 600.0;
    double expected_prob_a = 50.0 / 600.0;
    double expected_prob_f = 250.0 / 600.0;

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(600, 1, 2, JobType.T_JOB_SHORT);

      double sum = 0.0;
      double cumulative_prob = 0.0;
      for (Tuple<Double, HashSet<String>> tuple : schedule) {
        double probability = tuple.getFirst();
        HashSet<String> labels = tuple.getSecond();
        double actual_prob = probability - cumulative_prob;
        // the label is legit
        Assert.assertTrue(possibleLabels.containsAll(labels));
        if (labels.contains("d")) {
          if (sum + 300.0 <= 600.0) {
            Assert.assertEquals(expected_prob_d, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 300.0;
        } else if (labels.contains("e")) {
          if (sum + 220.0 <= 600.0) {
            Assert.assertEquals(expected_prob_e, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 220.0;
        } else if (labels.contains("g")) {
          if (sum + 100.0 <= 600.0) {
            Assert.assertEquals(expected_prob_g, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 100.0;
        } else if (labels.contains("a")) {
          if (sum + 50.0 <= 600.0) {
            Assert.assertEquals(expected_prob_a, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 50.0;
        } else if (labels.contains("f")) {
          if (sum + 300.0 <= 600.0) {
            Assert.assertEquals(expected_prob_f, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 300.0;
        }

        cumulative_prob = probability;
      }
    }
  }

  @Test
  public void shortJobFitsMultipleClassesProbabilistic() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    double highWeight = 10.0;
    double mediumWeight = 3.0;
    double lowWeight = 1.0;

    // start with probabilistic type selection disabled, and best-fit disabled
    UtilizationTable table = new UtilizationTable(true, lowWeight, mediumWeight,
                                                  highWeight, false, null);
    table.setUtilizationRecords(records);
    // job can fits in a single class
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("d", "e", "g", "a", "f", "b", "c"));
    double expected_prob_d = 300.0 / 600.0;
    double expected_prob_e = 220.0 / 600.0;
    double expected_prob_g = 100.0 / 600.0;
    double expected_prob_a = 50.0 / 600.0;
    double expected_prob_f = 250.0 / 600.0;
    double expected_prob_b = 100.0 / 600.0;
    double expected_prob_c = 300.0 / 600.0;

    int actual_occur_d = 0;
    int actual_occur_e = 0;
    int actual_occur_g = 0;
    int actual_occur_a = 0;
    int actual_occur_f = 0;
    int actual_occur_b = 0;
    int actual_occur_c = 0;

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(600, 1, 2, JobType.T_JOB_SHORT);

      double sum = 0.0;
      double cumulative_prob = 0.0;
      for (Tuple<Double, HashSet<String>> tuple : schedule) {
        double probability = tuple.getFirst();
        HashSet<String> labels = tuple.getSecond();
        double actual_prob = probability - cumulative_prob;
        // the label is legit
        Assert.assertTrue(possibleLabels.containsAll(labels));
        if (labels.contains("d")) {
          if (sum + 300.0 <= 600.0) {
            Assert.assertEquals(expected_prob_d, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 300.0;
          actual_occur_d += 1;
        } else if (labels.contains("e")) {
          if (sum + 220.0 <= 600.0) {
            Assert.assertEquals(expected_prob_e, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 220.0;
          actual_occur_e += 1;
        } else if (labels.contains("g")) {
          if (sum + 100.0 <= 600.0) {
            Assert.assertEquals(expected_prob_g, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 100.0;
          actual_occur_g += 1;
        } else if (labels.contains("a")) {
          if (sum + 50.0 <= 600.0) {
            Assert.assertEquals(expected_prob_a, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 50.0;
          actual_occur_a += 1;
        } else if (labels.contains("f")) {
          if (sum + 250.0 <= 600.0) {
            Assert.assertEquals(expected_prob_f, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 250.0;
          actual_occur_f += 1;
        } else if (labels.contains("b")) {
          if (sum + 100.0 <= 600.0) {
            Assert.assertEquals(expected_prob_b, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 100.0;
          actual_occur_b += 1;
        } else if (labels.contains("c")) {
          if (sum + 300.0 <= 600.0) {
            Assert.assertEquals(expected_prob_c, actual_prob, EPSILON);
          } else {
            Assert.assertEquals((600.0 - sum) / 600.0, actual_prob, EPSILON);
          }
          sum += 300.0;
          actual_occur_c += 1;
        }

        cumulative_prob = probability;
      }
    }

    Assert.assertTrue(actual_occur_a > 0);
    Assert.assertTrue(actual_occur_b > 0);
    Assert.assertTrue(actual_occur_c > 0);
    Assert.assertTrue(actual_occur_d > 0);
    Assert.assertTrue(actual_occur_e > 0);
    Assert.assertTrue(actual_occur_f > 0);
    Assert.assertTrue(actual_occur_g > 0);
  }

  @Test
  public void shortJobDoesNotFitDeterministic() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    // start with probabilistic type selection disabled, and best-fit disabled
    UtilizationTable table = new UtilizationTable(false, 1.0, 1.0, 1.0, false,
                                                  null);
    table.setUtilizationRecords(records);
    // job can fits in a single class
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
    double sum = 300.0 + 220.0 + 100.0 + 50.0 + 250.0 + 100.0 + 300.0;
    double expected_prob_d = 300.0 / sum;
    double expected_prob_e = 220.0 / sum;
    double expected_prob_g = 100.0 / sum;
    double expected_prob_a = 50.0 / sum;
    double expected_prob_f = 250.0 / sum;
    double expected_prob_b = 100.0 / sum;
    double expected_prob_c = 300.0 / sum;
    double actual_prob_a = 0.0;
    double actual_prob_b = 0.0;
    double actual_prob_c = 0.0;
    double actual_prob_d = 0.0;
    double actual_prob_e = 0.0;
    double actual_prob_f = 0.0;
    double actual_prob_g = 0.0;

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(10000, 1, 2, JobType.T_JOB_SHORT);
      // only pick one class
      Assert.assertEquals(1, schedule.size());
      double probability = schedule.get(0).getFirst();
      HashSet<String> labels = schedule.get(0).getSecond();
      // upper bound of the CDF is 1.0
      Assert.assertEquals(1.0, probability, 0.0);
      // the label is legit
      Assert.assertTrue(possibleLabels.containsAll(labels));

      if (labels.contains("a")) {
        actual_prob_a += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("b")) {
        actual_prob_b += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("c")) {
        actual_prob_c += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("d")) {
        actual_prob_d += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("e")) {
        actual_prob_e += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("f")) {
        actual_prob_f += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("g")) {
        actual_prob_g += 1.0 / NUM_ITERATIONS;
      }
    }

    // check the probability
    Assert.assertEquals(expected_prob_a, actual_prob_a, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_b, actual_prob_b, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_c, actual_prob_c, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_d, actual_prob_d, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_e, actual_prob_e, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_f, actual_prob_f, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_g, actual_prob_g, RANDOM_EPSILON);
  }

  @Test
  public void shortJobDoesNotFitProbabilistic() {
    UtilizationRecord[] records = getUtilizationRecordsForTest();

    double highWeight = 100.0;
    double mediumWeight = 2.0;
    double lowWeight = 1.0;

    // start with probabilistic type selection disabled, and best-fit disabled
    UtilizationTable table = new UtilizationTable(true, lowWeight,
                                                  mediumWeight, highWeight,
                                                  false, null);
    table.setUtilizationRecords(records);
    // job can fits in a single class
    HashSet<String> possibleLabels =
        new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
    double sum = (300.0 + 220.0 + 100.0) * highWeight +
        (50.0 + 250.0) * mediumWeight + (100.0 + 300.0) * lowWeight;
    double expected_prob_d = 300.0 * highWeight / sum;
    double expected_prob_e = 220.0 * highWeight / sum;
    double expected_prob_g = 100.0 * highWeight / sum;
    double expected_prob_a = 50.0 * mediumWeight / sum;
    double expected_prob_f = 250.0 * mediumWeight / sum;
    double expected_prob_b = 100.0 * lowWeight / sum;
    double expected_prob_c = 300.0 * lowWeight / sum;
    double actual_prob_a = 0.0;
    double actual_prob_b = 0.0;
    double actual_prob_c = 0.0;
    double actual_prob_d = 0.0;
    double actual_prob_e = 0.0;
    double actual_prob_f = 0.0;
    double actual_prob_g = 0.0;

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ArrayList<Tuple<Double, HashSet<String>>> schedule =
          table.pickClassesByProbability(10000, 1, 2, JobType.T_JOB_SHORT);
      // only pick one class
      Assert.assertEquals(1, schedule.size());
      double probability = schedule.get(0).getFirst();
      HashSet<String> labels = schedule.get(0).getSecond();
      // upper bound of the CDF is 1.0
      Assert.assertEquals(1.0, probability, 0.0);
      // the label is legit
      Assert.assertTrue(possibleLabels.containsAll(labels));

      if (labels.contains("a")) {
        actual_prob_a += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("b")) {
        actual_prob_b += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("c")) {
        actual_prob_c += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("d")) {
        actual_prob_d += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("e")) {
        actual_prob_e += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("f")) {
        actual_prob_f += 1.0 / NUM_ITERATIONS;
      } else if (labels.contains("g")) {
        actual_prob_g += 1.0 / NUM_ITERATIONS;
      }
    }

    // check the probability
    Assert.assertEquals(expected_prob_a, actual_prob_a, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_b, actual_prob_b, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_c, actual_prob_c, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_d, actual_prob_d, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_e, actual_prob_e, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_f, actual_prob_f, RANDOM_EPSILON);
    Assert.assertEquals(expected_prob_g, actual_prob_g, RANDOM_EPSILON);
  }

  @Test
  public void mediumJobFitsSingleClass() { }

  @Test
  public void mediumJobFitsMultipleClasses() { }
  
  @Test
  public void mediumJobDesoNotFit() { }

  @Test
  public void longJobFitsSingleClass() { }

  @Test
  public void longJobFitsMultipleClasses() { }

  @Test
  public void longJobDoesNotFit() { }
}
