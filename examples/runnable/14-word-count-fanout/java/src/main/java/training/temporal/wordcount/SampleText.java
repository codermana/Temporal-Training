package training.temporal.wordcount;

import java.util.List;

final class SampleText {
  private SampleText() {}

  static List<String> chunks() {
    return List.of(
        "Temporal workflows keep the overall document job durable.",
        "Each activity receives one chunk and counts its words.",
        "The workflow starts every chunk count before waiting for results.",
        "Fan-in sums the partial counts into one final total.");
  }
}
