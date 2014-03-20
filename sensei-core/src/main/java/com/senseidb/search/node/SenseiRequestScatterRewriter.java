package com.senseidb.search.node;

import java.util.Set;

import com.senseidb.search.req.SenseiRequest;

public interface SenseiRequestScatterRewriter {
  SenseiRequest rewrite(SenseiRequest origReq, Set<Integer> partitions);
}
