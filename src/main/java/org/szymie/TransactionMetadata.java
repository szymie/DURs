package org.szymie;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TransactionMetadata {
    List<Long> readings = new LinkedList<>();
    Map<Long, Long> updates = new HashMap<>();
}
