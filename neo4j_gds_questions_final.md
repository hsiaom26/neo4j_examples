
### 完成度自我評估表

| 題號 | 主題 | 核心演算法 | 難度 | 完成 |
|------|------|------------|------|------|
| 0 | 圖形中心性分析| Centrality, Betweeness | ⭐⭐ | ☐ |
| 1 | 多層次影響力 | PageRank, Betweenness, Degree | ⭐⭐⭐ | ☐ |
| 2 | 社群演化 | Louvain, 時序分析 | ⭐⭐⭐⭐ | ☐ |
| 3 | 弱連結分析 | Node Similarity, Link Prediction | ⭐⭐⭐ | ☐ |
| 4 | 多約束路徑 | Dijkstra, Yen's K-Shortest | ⭐⭐⭐⭐ | ☐ |
| 5 | 關鍵節點檢測 | Betweenness, Articulation Points | ⭐⭐⭐⭐ | ☐ |
| 6 | 旅遊規劃 | K-Means, MST, PageRank | ⭐⭐⭐⭐ | ☐ |
| 7 | 混合推薦 | Multi-algorithm fusion | ⭐⭐⭐⭐ | ☐ |
| 8 | 職涯推薦 | FastRP, Clustering, Link Prediction | ⭐⭐⭐⭐ | ☐ |
| 9 | 實時更新 | Incremental computation | ⭐⭐⭐⭐⭐ | ☐ |
| 10 | 詐欺環檢測 | Triangle Count, Label Propagation | ⭐⭐⭐⭐ | ☐ |
| 12 | 共謀網路 | K-Core, WCC, Risk propagation | ⭐⭐⭐⭐ | ☐ |
| 13 | 知識圖譜 | Node2Vec, Multi-hop reasoning | ⭐⭐⭐⭐⭐ | ☐ |
| 14 | 語義演化 | Bipartite projection, Conductance | ⭐⭐⭐⭐ | ☐ |
| 15 | 跨領域遷移 | Random Walk, Subgraph analysis | ⭐⭐⭐⭐ | ☐ |
| 16 | 記憶體優化 | Batch processing, Projection comparison | ⭐⭐⭐⭐⭐ | ☐ |
| 17 | 並行調度 | Concurrency optimization | ⭐⭐⭐⭐⭐ | ☐ |
| 18 | 漸進更新 | Delta-based computation | ⭐⭐⭐⭐⭐ | ☐ |


## 環境準備

```cypher
// 載入 Movies 數據集
:play movies
// 按照指示建立完整的 Movies graph

// 安裝 GDS Plugin (如果尚未安裝)
// 下載地址: https://neo4j.com/deployment-center/
```

---

## 第一部分:社交網路分析 (Movies Dataset)

### 題目 0: 
透過三種核心中心性指標：Degree、Betweenness、Closeness，將人物分為超級巨星、橋樑者、社交中心等類型並進行比較。

```cypher
CALL gds.graph.drop('movies-centrality-network', FALSE);

CALL gds.graph.project.cypher(
  'movies-centrality-network',
  // 節點查詢：選擇所有 Person
  'MATCH (p:Person) RETURN id(p) AS id',
  // 關係查詢：建立協作關係
  'MATCH (source:Person)-[:ACTED_IN|DIRECTED]->(m:Movie)<-[:ACTED_IN|DIRECTED]-(target:Person)
   WHERE id(source) < id(target)
   WITH source, target, count(DISTINCT m) AS collaborations
   RETURN id(source) AS source, id(target) AS target, collaborations AS weight'
)
YIELD graphName, nodeCount, relationshipCount, projectMillis
RETURN 
  graphName AS Graph,
  nodeCount AS TotalPeople,
  relationshipCount AS Collaborations,
  projectMillis AS BuildTimeMs;

// Step 3: 計算 Degree Centrality
CALL gds.degree.write('movies-centrality-network', {
  writeProperty: 'degreeCentrality'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'Degree Centrality' AS Metric,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// 查看 Top 10 Degree Centrality
MATCH (p:Person)
WHERE p.degreeCentrality IS NOT NULL
RETURN 
  p.name AS Person,
  p.degreeCentrality AS DegreeScore
ORDER BY p.degreeCentrality DESC
LIMIT 10;

// Step 4: 計算 Betweenness Centrality
CALL gds.betweenness.write('movies-centrality-network', {
  writeProperty: 'betweennessCentrality'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'Betweenness Centrality' AS Metric,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// 查看 Top 10 Betweenness Centrality
MATCH (p:Person)
WHERE p.betweennessCentrality IS NOT NULL
RETURN 
  p.name AS Person,
  round(p.betweennessCentrality * 100) / 100 AS BetweennessScore
ORDER BY p.betweennessCentrality DESC
LIMIT 10;

// Step 5: 計算 Closeness Centrality (使用 Harmonic Centrality)
CALL gds.closeness.harmonic.write('movies-centrality-network', {
  writeProperty: 'closenessCentrality'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'Closeness Centrality' AS Metric,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// 查看 Top 10 Closeness Centrality
MATCH (p:Person)
WHERE p.closenessCentrality IS NOT NULL
RETURN 
  p.name AS Person,
  round(p.closenessCentrality * 1000) / 1000 AS ClosenessScore
ORDER BY p.closenessCentrality DESC
LIMIT 10;

// ============================================
// Part 2: 綜合分析與分類
// ============================================

// Step 6: 標準化所有中心性指標（0-1 範圍）
MATCH (p:Person)
WHERE p.degreeCentrality IS NOT NULL
  AND p.betweennessCentrality IS NOT NULL
  AND p.closenessCentrality IS NOT NULL
WITH 
  max(p.degreeCentrality) AS maxDegree,
  max(p.betweennessCentrality) AS maxBetweenness,
max(p.closenessCentrality) AS maxCloseness
MATCH (p:Person)
WHERE p.degreeCentrality IS NOT NULL
SET 
  p.degreeNorm = toFloat(p.degreeCentrality) / maxDegree,
  p.betweennessNorm = toFloat(p.betweennessCentrality) / maxBetweenness,
  p.closenessNorm = toFloat(p.closenessCentrality) / maxCloseness
RETURN 
  'Normalization Complete' AS Status,
  count(p) AS NormalizedNodes;

// Step 7: 根據標準化指標進行人物分類
MATCH (p:Person)
WHERE p.degreeNorm IS NOT NULL
SET p.personalityType = CASE 
    WHEN p.degreeNorm > 0.6 AND p.betweennessNorm > 0.6 AND p.closenessNorm > 0.6 THEN 'Superstar'
    WHEN p.betweennessNorm > 0.7 AND p.degreeNorm >= 0.3 AND p.degreeNorm <= 0.7 THEN 'Bridge'
    WHEN p.degreeNorm > 0.7 AND p.betweennessNorm >= 0.3 AND p.betweennessNorm <= 0.7 THEN 'Social Hub'
    WHEN p.degreeNorm < 0.3 AND p.betweennessNorm < 0.3 AND p.closenessNorm < 0.3 THEN 'Specialist'
    ELSE 'Regular'
END
RETURN 'Classification Complete' AS Status,
       count(p) AS ClassifiedNodes;


// Step 8: 統計每種人物類型的數量
MATCH (p:Person)
WHERE p.personalityType IS NOT NULL
WITH p.personalityType AS type, count(p) AS count
RETURN 
  type AS PersonalityType,
  count AS NumberOfPeople,
  round(toFloat(count) / sum(count) * 100) AS Percentage
ORDER BY count DESC;

// Step 9: 查看每種類型的代表人物
MATCH (p:Person)
WHERE p.personalityType IS NOT NULL
WITH p.personalityType AS type, p
ORDER BY p.degreeNorm + p.betweennessNorm + p.closenessNorm DESC
WITH type, collect(p.name)[..3] AS representatives, count(p) AS totalCount
RETURN 
  type AS PersonalityType,
  totalCount AS Count,
  representatives AS TopRepresentatives
ORDER BY totalCount DESC;
```

### 題目 1: 多層次影響力分析

**情境**: 在電影社交網路中,需要識別最具影響力的演員,但不只是看直接連接數。

#### 解答程式碼:

```cypher
// Step 1: 建立圖投影 (Actor 之間通過電影的關係)
CALL gds.graph.project(
  'actor-network',
  'Person',
  {
    ACTED_WITH: {
      type: 'ACTED_IN',
      orientation: 'UNDIRECTED',
      properties: {}
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 2: 執行 PageRank
CALL gds.pageRank.write('actor-network', {
  writeProperty: 'pagerank',
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD nodePropertiesWritten, ranIterations;

// Step 3: 執行 Betweenness Centrality
CALL gds.betweenness.write('actor-network', {
  writeProperty: 'betweenness'
})
YIELD nodePropertiesWritten, computeMillis;

// Step 4: 執行 Degree Centrality
CALL gds.degree.write('actor-network', {
  writeProperty: 'degree'
})
YIELD nodePropertiesWritten;

// Step 5: 計算綜合影響力分數並標準化
MATCH (p:Person)
WHERE p.pagerank IS NOT NULL
WITH 
  max(p.pagerank) AS maxPR, 
  max(p.betweenness) AS maxBW, 
  max(p.degree) AS maxDeg
MATCH (p:Person)
WHERE p.pagerank IS NOT NULL
SET p.influenceScore = 
  (p.pagerank / maxPR) * 0.4 + 
  (p.betweenness / maxBW) * 0.35 + 
  (p.degree / maxDeg) * 0.25;

// Step 6: 找出 Top 10 最具綜合影響力的演員
MATCH (p:Person)
WHERE p.influenceScore IS NOT NULL
RETURN 
  p.name AS Actor,
  p.influenceScore AS CompositeScore,
  p.pagerank AS PageRank,
  p.betweenness AS Betweenness,
  p.degree AS Degree
ORDER BY p.influenceScore DESC
LIMIT 10;

// Step 7: 比較分析
MATCH (p:Person)
WHERE p.influenceScore IS NOT NULL
WITH p ORDER BY p.pagerank DESC LIMIT 10
WITH collect(p.name) AS topByPageRank
MATCH (p:Person)
WHERE p.influenceScore IS NOT NULL
WITH topByPageRank, p ORDER BY p.influenceScore DESC LIMIT 10
WITH topByPageRank, collect(p.name) AS topByComposite
RETURN 
  topByPageRank,
  topByComposite,
  [name IN topByPageRank WHERE name IN topByComposite] AS overlap;

// 清理
CALL gds.graph.drop('actor-network');
```

---

### 題目 2: 動態社群演化追蹤

**情境**: 分析不同年代電影合作網路的社群結構變化。

#### 解答程式碼:

```cypher
// Step 1: 為電影添加年代標籤 (Movies dataset 沒有 released，我們創建模擬數據)
MATCH (m:Movie)
SET m.released = toInteger(1990 + rand() * 30);

// Step 2: 建立三個時期的圖投影
// 時期 1: 1990前 (模擬為 1990-1999)
CALL gds.graph.project.cypher(
  'era1-network',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE m.released >= 1990 AND m.released < 2000
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount;

// 時期 2: 2000-2009
CALL gds.graph.project.cypher(
  'era2-network',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE m.released >= 2000 AND m.released < 2010
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount;

// 時期 3: 2010後
CALL gds.graph.project.cypher(
  'era3-network',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE m.released >= 2010
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount;

// Step 3: 對每個時期執行 Louvain
CALL gds.louvain.write('era1-network', {
  writeProperty: 'community_era1'
})
YIELD communityCount, modularity;

CALL gds.louvain.write('era2-network', {
  writeProperty: 'community_era2'
})
YIELD communityCount, modularity;

CALL gds.louvain.write('era3-network', {
  writeProperty: 'community_era3'
})
YIELD communityCount, modularity;

// Step 4: 追蹤演員的社群遷移
MATCH (p:Person)
WHERE p.community_era1 IS NOT NULL 
  AND p.community_era2 IS NOT NULL 
  AND p.community_era3 IS NOT NULL
RETURN 
  p.name AS Actor,
  p.community_era1 AS Era1Community,
  p.community_era2 AS Era2Community,
  p.community_era3 AS Era3Community,
  CASE 
    WHEN p.community_era1 <> p.community_era2 OR p.community_era2 <> p.community_era3 
    THEN 'Migrated' 
    ELSE 'Stable' 
  END AS Status
ORDER BY Status DESC, p.name
LIMIT 20;

// Step 5: 識別橋接者 (連接不同社群的演員)
MATCH (p:Person)
WHERE p.community_era1 IS NOT NULL 
  AND p.community_era2 IS NOT NULL
WITH p, p.community_era1 AS c1, p.community_era2 AS c2
WHERE c1 <> c2
MATCH (p)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(other:Person)
WHERE other.community_era1 IS NOT NULL 
  AND other.community_era2 IS NOT NULL
  AND other.community_era1 <> p.community_era1
WITH p, count(DISTINCT other) AS bridgeConnections
ORDER BY bridgeConnections DESC
LIMIT 10
RETURN 
  p.name AS Bridger,
  bridgeConnections AS CrossCommunityConnections;

// 清理
CALL gds.graph.drop('era1-network');
CALL gds.graph.drop('era2-network');
CALL gds.graph.drop('era3-network');
```

---

### 題目 3: 協作網路的弱連結強度

**情境**: 驗證「弱連結假說」在電影合作網路中的表現。

#### 解答程式碼:

```cypher
// Step 1: 建立演員網路圖投影
CALL gds.graph.project(
  'actor-collab-network',
  'Person',
  {
    ACTED_WITH: {
      orientation: 'UNDIRECTED'
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 2: 計算 Node Similarity (基於共同合作的電影)
CALL gds.nodeSimilarity.write('actor-collab-network', {
  writeRelationshipType: 'SIMILAR_TO',
  writeProperty: 'similarity',
  similarityCutoff: 0.1
})
YIELD nodesCompared, relationshipsWritten;

// Step 3: 將連結分為強弱連結
MATCH (p1:Person)-[s:SIMILAR_TO]->(p2:Person)
WITH percentileCont(s.similarity, 0.5) AS medianSimilarity
MATCH (p1:Person)-[s:SIMILAR_TO]->(p2:Person)
SET s.linkType = CASE 
  WHEN s.similarity >= medianSimilarity THEN 'Strong'
  ELSE 'Weak'
END;

// Step 4: 分析弱連結的橋接作用
MATCH (p1:Person)-[s:SIMILAR_TO {linkType: 'Weak'}]->(p2:Person)
MATCH (p1)-[:ACTED_IN]->(m1:Movie)
MATCH (p2)-[:ACTED_IN]->(m2:Movie)
WHERE m1 <> m2
WITH p1, p2, collect(DISTINCT m1.title) AS p1Movies, collect(DISTINCT m2.title) AS p2Movies
RETURN 
  p1.name AS Person1,
  p2.name AS Person2,
  size(p1Movies) AS P1MovieCount,
  size(p2Movies) AS P2MovieCount,
  size([m IN p1Movies WHERE m IN p2Movies]) AS SharedMovies
ORDER BY SharedMovies ASC
LIMIT 10;

// Step 5: 使用 Link Prediction 預測未來合作
CALL gds.graph.project(
  'actor-prediction-network',
  'Person',
  'ACTED_IN'
);

CALL gds.linkPrediction.adamicAdar.stream('actor-prediction-network')
YIELD node1, node2, score
WITH gds.util.asNode(node1) AS p1, gds.util.asNode(node2) AS p2, score
WHERE NOT EXISTS((p1)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2))
RETURN 
  p1.name AS Actor1,
  p2.name AS Actor2,
  score AS PredictionScore
ORDER BY score DESC
LIMIT 15;

// Step 6: 建立推薦系統
MATCH (actor:Person {name: 'Tom Hanks'})
CALL gds.nodeSimilarity.filtered.stream('actor-collab-network', {
  sourceNodes: [actor]
})
YIELD node1, node2, similarity
WITH gds.util.asNode(node2) AS similarActor, similarity
WHERE NOT EXISTS((actor)-[:ACTED_IN]->()<-[:ACTED_IN]-(similarActor))
RETURN 
  similarActor.name AS RecommendedCollaborator,
  similarity AS SimilarityScore
ORDER BY similarity DESC
LIMIT 10;

// 清理
CALL gds.graph.drop('actor-collab-network');
CALL gds.graph.drop('actor-prediction-network');
```

---

## 第二部分:路徑與最短路徑優化

### 題目 4: 多約束條件最短路徑

**情境**: 使用 Movies dataset 模擬物流網路 (演員=城市, 合作=路線)

#### 解答程式碼:

```cypher
// Step 1: 準備數據 - 為關係添加權重屬性
MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
WHERE id(p1) < id(p2)
WITH p1, p2, count(m) AS collaborations
MERGE (p1)-[r:CONNECTED_TO]-(p2)
SET r.distance = 100 + rand() * 400,
    r.travel_time = 1 + rand() * 5,
    r.toll_cost = 10 + rand() * 50;

// Step 2: 建立加權圖投影
CALL gds.graph.drop('weighted-network') YIELD graphName
  // ignore error if doesn't exist
  ;
CALL gds.graph.project(
  'weighted-network',
  'Person',
  {
    CONNECTED_TO: {
      orientation: 'UNDIRECTED',
      properties: ['distance', 'travel_time', 'toll_cost']
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// -----------------------------
// Helper: 選擇 degree 最大的 start / end 節點
// -----------------------------

// Step 3: 找出兩個起終點 (選擇有較多連接的演員) 並計算最短距離路徑
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end
// 如果資料集太小導致 end 為 null，請先確認至少有兩個 Person 節點
CALL gds.shortestPath.dijkstra.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  relationshipWeightProperty: 'distance'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN 
  'Shortest Distance' AS RouteType,
  gds.util.asNode(sourceNode).name AS Start,
  gds.util.asNode(targetNode).name AS End,
  totalCost AS TotalDistance,
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route;

// Step 4: 最短時間路徑（用相同的 start/end 選法）
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end
CALL gds.shortestPath.dijkstra.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  relationshipWeightProperty: 'travel_time'
})
YIELD index, totalCost, nodeIds
RETURN 
  'Shortest Time' AS RouteType,
  totalCost AS TotalTime,
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route;

// Step 5: 使用 Yen's K-Shortest Paths 找前5條路線
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end
CALL gds.shortestPath.yens.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  k: 5,
  relationshipWeightProperty: 'distance'
})
YIELD index, totalCost, nodeIds
RETURN 
  index AS RouteRank,
  totalCost AS TotalDistance,
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route,
  size(nodeIds) AS Hops;

// Step 6: 綜合評分最優路線（distance, travel_time, toll_cost）
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end

CALL gds.shortestPath.yens.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  k: 5,
  relationshipWeightProperty: 'distance'
})
YIELD index, totalCost AS distance, nodeIds, costs
// 對每條路徑展開鄰接邊以計算 travel_time 與 toll_cost 總和
UNWIND nodeIds AS gid
WITH index, distance, nodeIds, costs, collect(gid) AS gids
// 以相鄰 pair 計算邊的 sum：把 graph-id 轉成 db node 用 gds.util.asNode(...)
UNWIND range(0, size(gids)-2) AS i
WITH index, distance, gids, gds.util.asNode(gids[i]) AS p1, gds.util.asNode(gids[i+1]) AS p2
MATCH (p1)-[r:CONNECTED_TO]-(p2)
WITH index, distance, gids, sum(r.travel_time) AS totalTime, sum(r.toll_cost) AS totalCost
// 注意：因為我們 UNWIND 了每個路徑的 edge-segment，上面 sum() 是在每一組 index 下聚合的
WITH index, gids AS nodeIds, distance, totalTime, totalCost,
     (distance/500.0 * 0.33 + totalTime/6.0 * 0.33 + totalCost/60.0 * 0.34) AS compositeScore
RETURN 
  index AS RouteRank,
  distance AS TotalDistance,
  totalTime AS TotalTime,
  totalCost AS TotalCost,
  compositeScore AS CompositeScore,
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route
ORDER BY compositeScore ASC;

// 清理
CALL gds.graph.drop('weighted-network');

```

---

### 題目 5: 關鍵交通樞紐識別與容錯分析

**情境**: 識別網路中的關鍵節點並分析網路韌性。

#### 解答程式碼:

```cypher
// Step 1: 建立網路圖
CALL gds.graph.project(
  'hub-network',
  'Person',
  {
    CONNECTED_TO: {
      orientation: 'UNDIRECTED'
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 2: 使用 Betweenness Centrality 找關鍵樞紐
CALL gds.betweenness.write('hub-network', {
  writeProperty: 'betweenness_hub'
})
YIELD nodePropertiesWritten, computeMillis;

MATCH (p:Person)
WHERE p.betweenness_hub IS NOT NULL
RETURN 
  p.name AS Hub,
  p.betweenness_hub AS BetweennessCentrality
ORDER BY p.betweenness_hub DESC
LIMIT 20;

// Step 3: 計算基準網路的平均路徑長度
CALL gds.allShortestPaths.dijkstra.stream('hub-network', {
  relationshipWeightProperty: 'distance'
})
YIELD sourceNode, targetNode, totalCost
WITH avg(totalCost) AS avgPathLength, count(*) AS pathCount
RETURN 
  'Baseline Network' AS NetworkState,
  avgPathLength AS AveragePathLength,
  pathCount AS TotalPaths;

// Step 4: 找出 Articulation Points (斷點)
CALL gds.articleRank.write('hub-network', {
  writeProperty: 'articleRank'
})
YIELD nodePropertiesWritten;

MATCH (p:Person)
RETURN 
  p.name AS ArticulationPoint,
  p.articleRank AS Importance
ORDER BY p.articleRank DESC
LIMIT 10;

// Step 5: 模擬節點移除並計算影響
MATCH (critical:Person)
WHERE critical.betweenness_hub IS NOT NULL
WITH critical ORDER BY critical.betweenness_hub DESC LIMIT 5
WITH collect(critical) AS criticalNodes
UNWIND criticalNodes AS node
WITH node.name AS removedNode

// 建立不含該節點的子圖
CALL gds.graph.project.cypher(
  'reduced-network-' + removedNode,
  'MATCH (p:Person) WHERE p.name <> $nodeName RETURN id(p) AS id',
  'MATCH (p1:Person)-[r:CONNECTED_TO]-(p2:Person) 
   WHERE p1.name <> $nodeName AND p2.name <> $nodeName 
   RETURN id(p1) AS source, id(p2) AS target',
  {parameters: {nodeName: removedNode}}
)
YIELD graphName, nodeCount, relationshipCount
RETURN 
  removedNode AS RemovedHub,
  nodeCount AS RemainingNodes,
  relationshipCount AS RemainingConnections;

// Step 6: 建立城市重要性指數
MATCH (p:Person)
WHERE p.betweenness_hub IS NOT NULL
WITH max(p.betweenness_hub) AS maxBetweenness
MATCH (p:Person)
WHERE p.betweenness_hub IS NOT NULL
WITH p, maxBetweenness, 
     size((p)-[:CONNECTED_TO]-()) AS degree
SET p.importanceIndex = 
  (p.betweenness_hub / maxBetweenness) * 0.6 + 
  (degree / 50.0) * 0.4
RETURN 
  p.name AS Hub,
  p.importanceIndex AS ImportanceIndex,
  p.betweenness_hub AS Betweenness,
  degree AS Degree
ORDER BY p.importanceIndex DESC
LIMIT 15;

// 清理
CALL gds.graph.drop('hub-network');
```

---

### 題目 6: 旅遊路線規劃與聚類優化

**情境**: 設計「N日遊」路線推薦系統。

#### 解答程式碼:

```cypher
// Step 1: 建立帶座標的網路 (模擬地理位置)
MATCH (p:Person)
SET p.latitude = 40 + rand() * 10,
    p.longitude = -5 + rand() * 10;

MATCH (p:Person)
SET p.coordinate = [p.latitude, p.longitude];

// Step 2: 建立圖投影
CALL gds.graph.drop("tourism-network");
CALL gds.graph.project(
  'tourism-network',
  'Person',
  {
    CONNECTED_TO: {
      orientation: 'UNDIRECTED',
      properties: ['distance']
    }
  },
  {
    nodeProperties: ['latitude', 'longitude', 'coordinate']
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 3: 使用 K-Means 進行聚類
CALL gds.kmeans.write('tourism-network', {
  nodeProperty: 'coordinate',
  k: 5,
  writeProperty: 'cluster'
})
YIELD communityDistribution
RETURN communityDistribution;

// Step 4: 在每個聚類中找出 Minimum Spanning Tree
MATCH (p:Person)
WHERE p.cluster IS NOT NULL
WITH DISTINCT p.cluster AS clusterId
CALL {
  WITH clusterId
  MATCH (p:Person {cluster: clusterId})
　WITH collect(id(p)) AS clusterNodes, head(collect(id(p))) AS startNode, clusterId
  CALL gds.graph.drop("cluster-" + clusterId + "-graph", FALSE) YIELD nodeCount
  CALL gds.graph.project.cypher(
    'cluster-' + clusterId + '-graph',
    'MATCH (p:Person) WHERE id(p) IN $nodes RETURN id(p) AS id',
    'MATCH (p1:Person)-[r:CONNECTED_TO]-(p2:Person) 
     WHERE id(p1) IN $nodes AND id(p2) IN $nodes 
     RETURN id(p1) AS source, id(p2) AS target, r.distance AS weight',
    {parameters: {nodes: clusterNodes},
      relationshipOrientation: 'UNDIRECTED' 
    }
  )
  YIELD graphName
  
  CALL gds.spanningTree.write(graphName, {
    sourceNode: startNode,
    relationshipWeightProperty: 'weight',
    writeProperty: 'mst',
    writeRelationshipType: 'MST_ROUTE'
  })
  YIELD effectiveNodeCount
  
  RETURN effectiveNodeCount
}
RETURN clusterId, effectiveNodeCount;

// Step 5: 結合 PageRank 識別必訪城市
CALL gds.pageRank.write('tourism-network', {
  writeProperty: 'popularity',
  maxIterations: 20
})
YIELD nodePropertiesWritten;

MATCH (p:Person)
WHERE p.cluster IS NOT NULL AND p.popularity IS NOT NULL
WITH p.cluster AS cluster, p ORDER BY p.popularity DESC
WITH cluster, collect(p.name)[..3] AS topAttractions
RETURN 
  cluster AS TourismZone,
  topAttractions AS MustVisitCities;

// Step 6: 生成 3日、5日、7日遊路線
MATCH (p:Person)
WHERE p.cluster IS NOT NULL AND p.popularity IS NOT NULL
WITH p.cluster AS cluster, p ORDER BY p.popularity DESC
WITH cluster, collect(p)[..7] AS cities
RETURN 
  cluster AS Zone,
  [c IN cities[..3] | c.name] AS ThreeDayTour,
  [c IN cities[..5] | c.name] AS FiveDayTour,
  [c IN cities | c.name] AS SevenDayTour;

// Step 7: 設計環形路線 (簡化的 TSP)
MATCH (start:Person {cluster: 0})
WITH start ORDER BY start.popularity DESC LIMIT 1
MATCH path = (start)-[:MST_ROUTE*3..5]-(end)
WHERE end <> start
WITH start, end, path, length(path) AS pathLength
ORDER BY pathLength ASC
LIMIT 1
WITH nodes(path) AS tourNodes
RETURN [n IN tourNodes | n.name] AS CircularTour;

// 清理
CALL gds.graph.drop('tourism-network');
```
---

## 第三部分:推薦系統與相似度

### 題目 7: 混合推薦引擎

**情境**: 建立結合協同過濾和內容過濾的電影推薦系統。

#### 解答程式碼:

```cypher
// Step 1: 建立用戶-電影評分網路 (模擬)
MATCH (p:Person)
WITH p LIMIT 20
MATCH (m:Movie)
WITH p, m LIMIT 10
WHERE rand() < 0.3
MERGE (p)-[r:RATED]->(m)
SET r.rating = toInteger(1 + rand() * 5);

// Step 2: 建立圖投影
CALL gds.graph.project(
  'recommendation-network',
  ['Person', 'Movie'],
  {
    RATED: {
      properties: 'rating'
    },
    ACTED_IN: {},
    DIRECTED: {}
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 3: 計算用戶相似度 (協同過濾)
CALL gds.nodeSimilarity.write('recommendation-network', {
  nodeLabels: ['Person'],
  relationshipTypes: ['RATED'],
  writeRelationshipType: 'SIMILAR_USER',
  writeProperty: 'userSimilarity',
  similarityCutoff: 0.1
})
YIELD nodesCompared, relationshipsWritten;

// Step 4: 計算電影內容相似度
MATCH (m1:Movie)<-[:ACTED_IN]-(p:Person)-[:ACTED_IN]->(m2:Movie)
WHERE id(m1) < id(m2)
WITH m1, m2, count(p) AS commonActors
WITH m1, m2, commonActors,
     COUNT{((m1)<-[:ACTED_IN]-())} AS m1Actors,
     COUNT{((m2)<-[:ACTED_IN]-())} AS m2Actors
WITH m1, m2, 
     toFloat(commonActors) / (m1Actors + m2Actors - commonActors) AS jaccardSim
WHERE jaccardSim > 0.1
MERGE (m1)-[s:CONTENT_SIMILAR]-(m2)
SET s.similarity = jaccardSim;

// Step 5: Personalized PageRank 推薦
MATCH (user:Person {name: 'Tom Hanks'})
MATCH (user)-[:RATED]->(likedMovie:Movie)
WITH user, collect(id(likedMovie)) AS sourceNodes

CALL gds.pageRank.stream('recommendation-network', {
  sourceNodes: sourceNodes,
  dampingFactor: 0.85,
  maxIterations: 20
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
WHERE node:Movie AND NOT EXISTS((user)-[:RATED]->(node))
RETURN 
  node.title AS RecommendedMovie,
  score AS PersonalizedScore
ORDER BY score DESC
LIMIT 10;

// Step 6: 混合推薦 (結合三種方法)
MATCH (user:Person)
WITH user LIMIT 1
MATCH (user)-[:RATED {rating: 5}]->(likedMovie:Movie)

// 協同過濾推薦
MATCH (user)-[:SIMILAR_USER]->(similarUser:Person)
MATCH (similarUser)-[r:RATED]->(movie:Movie)
WHERE NOT EXISTS((user)-[:RATED]->(movie)) AND r.rating >= 4
WITH user, movie, avg(r.rating) AS cfScore

// 內容過濾推薦
MATCH (user)-[:RATED {rating: 5}]->(liked:Movie)
MATCH (liked)-[s:CONTENT_SIMILAR]-(movie)
WITH user, movie, cfScore, avg(s.similarity) AS contentScore

// 個性化 PageRank
MATCH (user)-[:RATED]->(likedMovie:Movie)
WITH user, movie, cfScore, contentScore, collect(id(likedMovie)) AS sourceNodes
CALL gds.pageRank.stream('recommendation-network', {
  sourceNodes: sourceNodes,
  maxIterations: 10
})
YIELD nodeId, score
WHERE nodeId = id(movie)
WITH user, movie, cfScore, contentScore, score AS prScore

// 綜合評分
WITH movie,
     cfScore,
     contentScore,
     prScore,
     coalesce(cfScore, 0) * 0.35 +
     coalesce(contentScore, 0) * 0.3 +
     coalesce(prScore, 0) * 0.35 AS hybridScore
RETURN 
  movie.title AS Movie,
  hybridScore AS HybridScore,
  cfScore AS CollaborativeScore,
  contentScore AS ContentScore,
  prScore AS PersonalizedPageRank
ORDER BY hybridScore DESC
LIMIT 15;

// Step 7: 冷啟動策略 - 熱門且多樣化推薦
CALL gds.pageRank.write('recommendation-network', {
  writeProperty: 'globalPopularity'
})
YIELD nodePropertiesWritten;

MATCH (m:Movie)
WHERE m.globalPopularity IS NOT NULL
WITH m ORDER BY m.globalPopularity DESC LIMIT 50
// 選擇多樣化的電影
WITH collect(m) AS popularMovies
UNWIND range(0, 9) AS idx
WITH popularMovies[idx * 5] AS diverseMovie
RETURN 
  diverseMovie.title AS ColdStartRecommendation,
  diverseMovie.globalPopularity AS Popularity
ORDER BY diverseMovie.globalPopularity DESC;

// 清理
CALL gds.graph.drop('recommendation-network');
```

---

### 題目 8: 演員職涯路徑推薦

**情境**: 為新進演員推薦最佳職涯發展路徑。

#### 解答程式碼:

```cypher
// Step 1: 準備職涯成功指標
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
WITH p, count(m) AS movieCount
WHERE movieCount >= 2
SET p.careerSuccess = movieCount * (1 + rand());

// Step 2: 使用新的 Cypher projection 建立職涯網路
MATCH (source:Person)-[r:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(target:Person)
WHERE source.careerSuccess IS NOT NULL AND target.careerSuccess IS NOT NULL
RETURN gds.graph.project(
  'career-network',
  source,
  target,
  {
    sourceNodeProperties: source { .careerSuccess },
    targetNodeProperties: target { .careerSuccess }
  },
  {
    undirectedRelationshipTypes: ['*']
  }
);

// Step 3: 使用 FastRP 生成職涯向量嵌入
CALL gds.fastRP.write('career-network', {
  embeddingDimension: 128,
  iterationWeights: [0.0, 1.0, 1.0, 1.0],
  randomSeed: 42,
  writeProperty: 'careerEmbedding'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'FastRP Embeddings' AS Feature,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// Step 4: 使用 Leiden (比 Louvain 更準確) 進行聚類
CALL gds.leiden.write('career-network', {
  writeProperty: 'careerPathType',
  includeIntermediateCommunities: false,
  randomSeed: 42
})
YIELD communityCount, modularity, computeMillis
RETURN 
  'Career Path Clustering' AS Analysis,
  communityCount AS PathTypes,
  round(modularity * 1000) / 1000 AS Modularity,
  computeMillis AS TimeMs;

// Step 5: 分析每個職涯類型的特徵
MATCH (p:Person)
WHERE p.careerPathType IS NOT NULL
WITH p.careerPathType AS pathType, p
ORDER BY p.careerSuccess DESC
WITH pathType,
     collect(p.name)[..5] AS topPerformers,
     avg(p.careerSuccess) AS avgSuccess,
     count(p) AS memberCount
RETURN 
  pathType AS CareerPathType,
  topPerformers AS SuccessExamples,
  round(avgSuccess * 10) / 10 AS AvgSuccessScore,
  memberCount AS PathMembers
ORDER BY avgSuccess DESC;

// Step 6: 使用 Harmonic Centrality 識別關鍵連結者
CALL gds.closeness.harmonic.write('career-network', {
  writeProperty: 'networkInfluence'
})
YIELD centralityDistribution, computeMillis;

MATCH (p:Person)
WHERE p.networkInfluence IS NOT NULL AND p.careerSuccess > 5
RETURN 
  p.name AS KeyConnector,
  p.careerPathType AS PathType,
  round(p.networkInfluence * 1000) / 1000 AS Influence,
  round(p.careerSuccess * 10) / 10 AS Success
ORDER BY p.networkInfluence DESC
LIMIT 15;

// Step 7: 為新演員找相似成功路徑 (使用嵌入相似度)
MATCH (newActor:Person)
WHERE newActor.careerSuccess IS NULL OR newActor.careerSuccess < 3
WITH newActor LIMIT 3

MATCH (successful:Person)
WHERE successful.careerSuccess > 8
  AND successful.careerEmbedding IS NOT NULL
  AND newActor.careerEmbedding IS NOT NULL
WITH newActor, successful,
     gds.similarity.cosine(newActor.careerEmbedding, successful.careerEmbedding) AS similarity
WHERE similarity > 0.5
ORDER BY similarity DESC
LIMIT 5
RETURN 
  newActor.name AS EmergingActor,
  collect({
    mentor: successful.name,
    similarity: round(similarity * 1000) / 1000,
    pathType: successful.careerPathType,
    successLevel: round(successful.careerSuccess * 10) / 10
  }) AS SuggestedRoleModels;

// Step 8: Link Prediction - 預測有價值的合作
MATCH (person1:Person)
WHERE person1.careerSuccess > 5
MATCH (person2:Person)
WHERE person2.careerSuccess > 5 
  AND id(person1) < id(person2)
  AND NOT EXISTS((person1)-[:ACTED_IN]->()<-[:ACTED_IN]-(person2))
WITH person1, person2, 
     gds.alpha.linkprediction.adamicAdar(person1, person2) AS adamicScore
WHERE adamicScore > 0
RETURN 
  person1.name AS Actor1,
  person2.name AS Actor2,
  round(adamicScore * 100) / 100 AS CollaborationPotential,
  person1.careerPathType AS Path1,
  person2.careerPathType AS Path2,
  CASE 
    WHEN person1.careerPathType <> person2.careerPathType 
    THEN 'Cross-Path Opportunity'
    ELSE 'Same-Path Collaboration'
  END AS CollaborationType
ORDER BY adamicScore DESC
LIMIT 20;

// Step 9: 職涯軌跡分析 - 使用 Random Walk
MATCH (mentor:Person)
WHERE mentor.careerSuccess > 10
WITH mentor ORDER BY mentor.networkInfluence DESC LIMIT 3

CALL gds.randomWalk.stream('career-network', {
  sourceNodes: [mentor],
  walkLength: 8,
  walksPerNode: 5,
  randomSeed: 42
})
YIELD nodeIds, path
WITH [nodeId IN nodeIds | gds.util.asNode(nodeId)] AS pathNodes
WHERE size(pathNodes) >= 5
WITH [n IN pathNodes WHERE n:Person | 
  {name: n.name, pathType: n.careerPathType}] AS careerJourney
WHERE size(careerJourney) >= 3
RETURN 
  careerJourney[0].name AS StartingPoint,
  [step IN careerJourney[1..-1] | step.name] AS CareerProgression,
  size(REDUCE(acc = [], s IN careerJourney |
    CASE WHEN NOT s.pathType IN acc THEN acc + s.pathType ELSE acc END
  )) AS PathDiversity // 對 careerJourney 中的每個元素取出其 pathType 屬性，再計算其中不同 pathType 的數量。
LIMIT 10;

// Step 10: 生成個性化職涯建議報告
MATCH (actor:Person)
WHERE actor.careerSuccess IS NULL OR actor.careerSuccess < 5
WITH actor LIMIT 1

// 找相似的成功者
MATCH (successful:Person)
WHERE successful.careerSuccess > 8
  AND successful.careerEmbedding IS NOT NULL
  AND actor.careerEmbedding IS NOT NULL
WITH actor, successful,
     gds.similarity.cosine(actor.careerEmbedding, successful.careerEmbedding) AS similarity
ORDER BY similarity DESC
LIMIT 3
WITH actor, collect(successful) AS mentors

MATCH (person2:Person)
WHERE person2 <> actor
WITH actor, mentors,
    person2 AS potentialPartner,
    gds.alpha.linkprediction.adamicAdar(actor, person2) AS score
ORDER BY score DESC
LIMIT 5
RETURN 
  actor.name AS Actor,
  actor.careerPathType AS CurrentPath,
  [m IN mentors | {
    name: m.name,
    path: m.careerPathType,
    success: round(m.careerSuccess * 10) / 10
  }] AS RoleModels,
  collect({
    partner: potentialPartner.name,
    score: round(score * 100) / 100
  }) AS RecommendedCollaborations;

// 清理
CALL gds.graph.drop('career-network');
```

---

### 題目 9: 實時推薦更新機制

**情境**: 當用戶行為發生時,即時更新推薦結果。

#### 解答程式碼:

```cypher
// Step 0: 先刪乾淨（安全刪除）
CALL gds.graph.drop('realtime-rec-network', FALSE);
CALL gds.graph.drop('person-cograph', FALSE); 

// Step 1: 建立 in-memory 圖投影（realtime-rec-network）
// 這個圖可以保留 Person 與 Movie，以及 REVIEWED(無向) 與 ACTED_IN(明確設定)
CALL gds.graph.project(
  'realtime-rec-network',
  ['Person', 'Movie'],
  {
      REVIEWED: {
          properties: 'rating',
          orientation: 'UNDIRECTED'    // 明確無向
      },
      ACTED_IN: {
          orientation: 'NATURAL'      // 或 'UNDIRECTED'，視你是否需要保留方向
      }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 2: 為 Triangle Count 建立一個「Person-Person 共評分投影」(undirected)
// 因為原始 graph 是 bipartite(Person-Movie)，TriangleCount 必須在 Person-Person 的投影上執行

// 建立新的共評分關係（以共同評分電影數作為權重）
MATCH (p1:Person)-[:REVIEWED]->(m:Movie)<-[:REVIEWED]-(p2:Person)
WHERE id(p1) < id(p2)  // 這裡確保每對節點順序固定，避免重複或不被 CREATE
WITH p1, p2, count(*) AS weight
MERGE (p1)-[r:CO_REVIEWED_WITH]-(p2)
SET r.weight = weight;

CALL gds.graph.project(
  'person-cograph',
  ['Person'],
  {
    CO_REVIEWED_WITH: {
      properties: 'weight',
      orientation: 'UNDIRECTED'
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// Step 3: 計算 Triangle Count 並寫回 DB（write 模式會把屬性寫回 Neo4j）
CALL gds.triangleCount.write('person-cograph', {
  writeProperty: 'triangles_baseline'
})
YIELD nodePropertiesWritten, globalTriangleCount, nodeCount;

// 你可以看前 10 名
MATCH (p:Person)
RETURN p.name AS User, p.triangles_baseline AS TriangleCount
ORDER BY p.triangles_baseline DESC
LIMIT 10;

// Step 4: 模擬用戶新增評分
MATCH (user:Person)
WITH user LIMIT 1
MATCH (movie:Movie)
WHERE NOT EXISTS((user)-[:REVIEWED]->(movie))
WITH user, movie LIMIT 1
MERGE (user)-[r:REVIEWED]->(movie)
SET r.rating = 5, r.timestamp = timestamp()
WITH user, movie

// Step 5: 增量更新 - 只更新受影響的節點
MATCH (user)-[:REVIEWED]->(newMovie:Movie)
WHERE newMovie = movie
MATCH (user)-[:REVIEWED]->(otherMovie:Movie)<-[:REVIEWED]-(otherUser:Person)
WHERE otherMovie <> newMovie
WITH user, collect(DISTINCT otherUser) AS affectedUsers

UNWIND affectedUsers AS affected
WITH user, affected
// 重新計算受影響用戶的相似度
MATCH (affected)-[:REVIEWED]->(m1:Movie)<-[:REVIEWED]-(user)
WITH user, affected, count(m1) AS commonMovies,
     COUNT{(affected)-[:REVIEWED]->()} AS affectedTotal,
     COUNT{(user)-[:REVIEWED]->()} AS userTotal
WITH user, affected, 
     toFloat(commonMovies) / sqrt(affectedTotal * userTotal) AS newSimilarity
MERGE (user)-[s:SIMILAR_TO_INCREMENTAL]-(affected)
SET s.similarity = newSimilarity,
    s.updated = timestamp()
RETURN 
  user.name AS User,
  collect({user: affected.name, similarity: newSimilarity})[..5] AS UpdatedSimilarities;

// Step 6: 快取策略 - 只對鄰近節點重新計算
MATCH (user:Person)
WITH user LIMIT 1
MATCH (user)-[:REVIEWED]->(:Movie)<-[:REVIEWED]-(neighbor:Person)
WITH user, collect(DISTINCT neighbor) AS neighbors

UNWIND neighbors AS neighbor
MATCH (neighbor)-[r:REVIEWED]->(m:Movie)
WHERE NOT EXISTS((user)-[:REVIEWED]->(m)) AND r.rating >= 4
WITH user, m, avg(r.rating) AS avgRating, count(*) AS ratingCount
RETURN 
  m.title AS IncrementalRecommendation,
  avgRating AS Score,
  ratingCount AS SupportingUsers
ORDER BY avgRating DESC, ratingCount DESC
LIMIT 10;

// Step 7: 性能比較 - 全圖重算 vs 增量更新
WITH timestamp() AS startTime
CALL gds.nodeSimilarity.write('realtime-rec-network', {
  nodeLabels: ['Person'],
  relationshipTypes: ['REVIEWED'],
  writeRelationshipType: 'FULL_RECALC',
  writeProperty: 'similarity'
})
YIELD nodesCompared, computeMillis
RETURN 
  'Full Recalculation' AS Method,
  computeMillis AS TimeMs,
  nodesCompared AS NodesProcessed;

RETURN 
  'Incremental Update' AS Method,
  'Much Faster' AS TimeMs,
  'Only Affected Nodes' AS NodesProcessed;

// 最後清理
CALL gds.graph.drop('person-cograph');
CALL gds.graph.drop('realtime-rec-network');
```

---

## 第四部分:異常檢測與詐欺偵測

### 題目 10: 多層次詐欺環檢測

**情境**: 在交易網路中檢測複雜的詐欺模式。

#### 解答程式碼:

```cypher
// Step 1: 使用 Movies dataset 模擬交易網路
CREATE (p)-[r:RATED]->(m)
SET r.amount = toInteger(10 + rand() * 990),
    r.timestamp = timestamp() - toInteger(rand() * 86400000 * 30);

// 標記一些可疑帳戶 (模擬)
MATCH (p:Person)
WITH p, rand() AS r
WHERE r < 0.1
SET p.suspicious = true;

// ✅ Step 2: 建立交易網路圖投影（容錯）
CALL gds.graph.drop('fraud-detection-network', false);
CALL gds.graph.project(
  'fraud-detection-network',
  'Person',
  {
    RATED: {
      properties: ['amount', 'timestamp'],
      orientation: 'UNDIRECTED'
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 3: Triangle Count 檢測基礎詐欺環
CALL gds.triangleCount.write('fraud-detection-network', {
  writeProperty: 'triangles'
})
YIELD nodeCount, globalTriangleCount;

MATCH (p:Person)
WHERE p.triangles > 0
RETURN 
  p.name AS Account,
  p.triangles AS TriangleCount,
  p.suspicious AS IsSuspicious
ORDER BY p.triangles DESC
LIMIT 15;

// Step 4: 檢測更長的環 (4-6 節點)
// Step 4: 純 Cypher 檢測 4–6 節點環
MATCH path = (p1:Person)-[:RATED*4..6]-(p1)
WHERE ALL(node IN nodes(path) WHERE node:Person)
WITH path, nodes(path) AS nds
// 確認是 simple cycle（起點與終點相同，其餘節點不重複）
WHERE head(nds) = last(nds)
  AND size(nds) = size( [x IN nds | x] )  // redundant for clarity
  AND size(nds) = size( [x IN nds | x] ) + 1
WITH nds AS cycle, length(path) AS cycleLength
WHERE cycleLength >= 4
RETURN 
  [n IN cycle | n.name] AS FraudRing,
  cycleLength AS RingSize
LIMIT 10;

// Step 5: Label Propagation 識別詐欺社群
CALL gds.labelPropagation.write('fraud-detection-network', {
  writeProperty: 'community',
  maxIterations: 10
})
YIELD communityCount, ranIterations;

MATCH (p:Person)
WHERE p.community IS NOT NULL AND p.suspicious = true
WITH p.community AS suspiciousCommunity
MATCH (member:Person {community: suspiciousCommunity})
RETURN 
  suspiciousCommunity AS FraudCommunity,
  collect(member.name) AS Members,
  size(collect(member)) AS MemberCount
ORDER BY MemberCount DESC
LIMIT 5;

// Step 6: 異常評分系統
MATCH (p:Person)
OPTIONAL MATCH (p)-[r:RATED]->(:Movie)
WITH p, 
     coalesce(p.triangles, 0) AS triangles,
     count(r) AS transactionCount,
     avg(r.amount) AS avgAmount,
     stDev(r.amount) AS stdAmount
WITH p, triangles, transactionCount, avgAmount, stdAmount,
     CASE 
       WHEN triangles > 5 THEN 0.4
       WHEN triangles > 2 THEN 0.2
       ELSE 0.0
     END AS triangleScore,
     CASE 
       WHEN transactionCount > 10 THEN 0.3
       WHEN transactionCount > 5 THEN 0.15
       ELSE 0.0
     END AS frequencyScore,
     CASE 
       WHEN stdAmount > 300 THEN 0.3
       WHEN stdAmount > 150 THEN 0.15
       ELSE 0.0
     END AS amountScore
SET p.fraudScore = triangleScore + frequencyScore + amountScore
RETURN 
  p.name AS Account,
  p.fraudScore AS FraudScore,
  triangles AS Triangles,
  transactionCount AS Transactions,
  round(avgAmount) AS AvgAmount,
  round(stdAmount) AS StdDevAmount,
  p.suspicious AS ActuallySuspicious
ORDER BY p.fraudScore DESC
LIMIT 20;

// 清理
CALL gds.graph.drop('fraud-detection-network');
```


---

### 題目 12: 共謀網路挖掘

**情境**: 識別互相配合進行詐欺的帳戶群組。

#### 解答程式碼:

```cypher
// Step 1: 計算加權 Degree Centrality (基於交易金額)
CALL gds.graph.drop("collusion-network");
CALL gds.graph.project(
  'collusion-network',
  'Person',
  {
      RATED: {
          properties: 'amount',
          orientation: 'UNDIRECTED'
      }
  }
)
YIELD graphName, nodeCount, relationshipCount;

CALL gds.degree.write('collusion-network', {
  relationshipWeightProperty: 'amount',
  writeProperty: 'weightedDegree'
})
YIELD nodePropertiesWritten;

// 識別高風險節點
MATCH (p:Person)
WHERE p.weightedDegree IS NOT NULL
WITH percentileDisc(p.weightedDegree, 0.9) AS threshold
MATCH (highrisk:Person)
WHERE highrisk.weightedDegree >= threshold
SET highrisk.highRisk = true
RETURN 
  highrisk.name AS HighRiskAccount,
  highrisk.weightedDegree AS WeightedDegree
ORDER BY highrisk.weightedDegree DESC
LIMIT 20;

// Step 2: Ego Graph 提取 (2-hop 鄰居)
MATCH (highrisk:Person {highRisk: true})
WITH highrisk LIMIT 5
MATCH path = (highrisk)-[:RATED*1..2]->(neighbor)
WITH highrisk, collect(DISTINCT neighbor) AS neighbors
RETURN 
  highrisk.name AS CenterNode,
  [n IN neighbors | CASE WHEN n:Person THEN n.name ELSE n.title END] AS EgoNetwork,
  size(neighbors) AS NetworkSize
ORDER BY NetworkSize DESC;

// Step 3: K-Core Decomposition 找緊密群組
CALL gds.kcore.write('collusion-network', {
  writeProperty: 'coreValue'
})
YIELD nodePropertiesWritten, degeneracy;

MATCH (p:Person)
WHERE p.coreValue IS NOT NULL AND p.coreValue >= 3
WITH p.coreValue AS coreLevel, collect(p.name) AS members
RETURN 
  coreLevel AS KCoreLevel,
  members AS CollusionGroup,
  size(members) AS GroupSize
ORDER BY coreLevel DESC
LIMIT 10;

// Step 4: Weakly Connected Components 識別孤立詐欺網路
CALL gds.wcc.write('collusion-network', {
  writeProperty: 'wccComponent'
})
YIELD componentCount, componentDistribution;

MATCH (p:Person)
WHERE p.wccComponent IS NOT NULL
WITH p.wccComponent AS component, collect(p) AS members
WHERE size(members) >= 3 AND size(members) <= 10
WITH component, members,
     [m IN members WHERE m.highRisk = true | m.name] AS highRiskMembers
WHERE size(highRiskMembers) > 0
RETURN 
  component AS IsolatedFraudNetwork,
  [m IN members | m.name] AS AllMembers,
  highRiskMembers AS HighRiskMembers,
  size(members) AS NetworkSize
ORDER BY size(highRiskMembers) DESC
LIMIT 10;

// Step 5: 風險傳播模型 - BFS 傳播距離 (新版 GDS 用法)
MATCH (knownFraud:Person)
WHERE knownFraud.suspicious = true OR knownFraud.highRisk = true
WITH collect(id(knownFraud)) AS fraudNodes

UNWIND fraudNodes AS sourceNode
CALL gds.bfs.stream('collusion-network', {
  sourceNode: sourceNode,
  maxDepth: 4
})
YIELD nodeIds, path
UNWIND nodeIds AS nodeId
WITH DISTINCT nodeId, min(size(nodes(path))) AS minDistance
WITH gds.util.asNode(nodeId) AS person, minDistance
WHERE person.suspicious IS NULL AND person.highRisk IS NULL
SET person.fraudProximity = minDistance
RETURN 
  person.name AS Account,
  minDistance AS DistanceFromKnownFraud,
  CASE 
    WHEN minDistance = 1 THEN 'Very High Risk'
    WHEN minDistance = 2 THEN 'High Risk'
    WHEN minDistance = 3 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END AS RiskLevel
ORDER BY minDistance ASC, person.name
LIMIT 20;

// Step 6: 綜合風險評分
MATCH (p:Person)
WHERE p.weightedDegree IS NOT NULL
WITH max(p.weightedDegree) AS maxDegree,
     max(coalesce(p.coreValue, 0)) AS maxCore
MATCH (p:Person)
SET p.collusionRiskScore = 
  (coalesce(p.weightedDegree, 0) / maxDegree) * 0.3 +
  (coalesce(p.coreValue, 0) / maxCore) * 0.3 +
  (CASE 
    WHEN p.fraudProximity = 1 THEN 0.4
    WHEN p.fraudProximity = 2 THEN 0.3
    WHEN p.fraudProximity = 3 THEN 0.2
    ELSE 0.1
  END)
RETURN 
  p.name AS Account,
  round(p.collusionRiskScore * 100) / 100 AS CollusionRisk,
  p.weightedDegree AS TransactionVolume,
  p.coreValue AS KCore,
  p.fraudProximity AS ProximityToFraud
ORDER BY p.collusionRiskScore DESC
LIMIT 25;

// 清理
CALL gds.graph.drop('collusion-network');
```

---

## 第五部分:知識圖譜與語義分析

### 題目 13: 電影知識圖譜推理

**情境**: 建立能進行多跳推理的電影知識系統。

#### 解答程式碼:

```cypher
// Step 1: 建立知識圖譜關係
// ===============================
// 主題相似 (共享演員 >=2)
MATCH (m1:Movie)<-[:ACTED_IN]-(p:Person)-[:ACTED_IN]->(m2:Movie)
WHERE id(m1) < id(m2)
WITH m1, m2, count(p) AS commonActors
WHERE commonActors >= 2
MERGE (m1)-[s:SIMILAR_THEME]-(m2)
SET s.strength = commonActors;

// 影響關係 (時間 + 人員)
MATCH (older:Movie)<-[:ACTED_IN|DIRECTED]-(creator)-[:ACTED_IN|DIRECTED]->(newer:Movie)
WHERE older.released < newer.released 
AND (newer.released - older.released) > 1 AND  (newer.released - older.released) < 15
WITH older, newer, count(DISTINCT creator) AS influence
WHERE influence >= 1
MERGE (newer)-[i:INFLUENCED_BY]->(older)
SET i.weight = influence;

// Step 2: 建立多關係圖投影
// ===============================
CALL gds.graph.drop("knowledge-graph-v2", FALSE);
CALL gds.graph.project(
    'knowledge-graph-v2',
    ['Person','Movie'],
    {
        ACTED_IN: {orientation: 'UNDIRECTED'},
        DIRECTED: {orientation: 'UNDIRECTED'},
        SIMILAR_THEME: {orientation: 'UNDIRECTED'},
        INFLUENCED_BY: {orientation: 'NATURAL'}
    }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 3: 最短路徑分析 (Tom Hanks -> Kevin Bacon)
// ===============================
MATCH (start:Person {name:'Tom Hanks'}), (end:Person {name:'Kevin Bacon'})
CALL gds.allShortestPaths.dijkstra.stream('knowledge-graph-v2', {
    sourceNode: start,
    relationshipTypes: ['ACTED_IN']})
YIELD totalCost, path
WITH [n IN nodes(path) | n.name] AS pathNodes, totalCost
RETURN pathNodes AS BaconPath, totalCost AS Distance
LIMIT 5;


// Step 4: Node2Vec 嵌入學習
// ===============================
CALL gds.node2vec.write('knowledge-graph-v2', {
    embeddingDimension: 128,
    walkLength: 80,
    walksPerNode: 10,
    inOutFactor: 0.5,
    returnFactor: 0.5,
    randomSeed: 42,
    writeProperty: 'kg_embedding'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 'Node2Vec Embeddings' AS Feature,
       nodePropertiesWritten AS NodesEmbedded,
       computeMillis AS TimeMs;

// Step 5: 基於嵌入的相似度推薦
// ===============================
MATCH (m1:Movie), (m2:Movie)
WHERE m1 <> m2 AND m1.kg_embedding IS NOT NULL AND m2.kg_embedding IS NOT NULL
WITH m1, m2, gds.similarity.cosine(m1.kg_embedding, m2.kg_embedding) AS sim
WHERE sim > 0.6
RETURN m1.title AS Movie, m2.title AS SimilarMovie, round(sim*1000)/1000 AS CosineSim
ORDER BY sim DESC
LIMIT 15;

// Step 6: 知識樞紐分析 (Harmonic Centrality)
// ===============================
CALL gds.closeness.harmonic.write('knowledge-graph-v2', {
    writeProperty: 'knowledgeHub'
})
YIELD centralityDistribution, computeMillis;

MATCH (m:Movie)
WHERE m.knowledgeHub IS NOT NULL
RETURN m.title AS Movie, round(m.knowledgeHub*1000)/1000 AS HubScore
ORDER BY m.knowledgeHub DESC
LIMIT 15;

// Step 7: 清理 in-memory 圖
// ===============================
CALL gds.graph.drop('knowledge-graph-v2');
```

---

### 題目 14: 語義社群演化

**情境**: 分析電影主題和類型的演化趨勢。

#### 解答程式碼:

```cypher
// Step 1: 為電影添加類型標籤 (模擬 genres)
MATCH (m:Movie)
WITH m, ['Action', 'Drama', 'Comedy', 'Sci-Fi', 'Thriller', 'Romance'] AS genres
SET m.genre = genres[toInteger(rand() * size(genres))];

// Step 2: 建立二分圖 Movie-Genre 並投影為單模圖
CALL gds.graph.drop("genre-network", FALSE);
CALL gds.graph.project(
    'genre-network',
    ['Movie', 'Genre'],                  // 節點標籤
    {
        HAS_GENRE: {
            orientation: 'UNDIRECTED'    // 無向圖
        }
    }
)
YIELD graphName, nodeCount, relationshipCount;


// 創建模擬的 Genre 節點和關係
MATCH (m:Movie)
WHERE m.genre IS NOT NULL
MERGE (g:Genre {name: m.genre})
MERGE (m)-[:HAS_GENRE]->(g);

// 投影為類型-類型網路 (基於共現)
MATCH (g1:Genre)<-[:HAS_GENRE]-(m:Movie)-[:HAS_GENRE]->(g2:Genre)
WHERE id(g1) < id(g2)
WITH g1, g2, count(m) AS weight
MERGE (g1)-[r:CO_OCCUR]-(g2)
SET r.weight = weight;
CALL gds.graph.drop("genre-cooccurrence", FALSE);
CALL gds.graph.project(
    'genre-cooccurrence',
    'Genre',
    'CO_OCCUR'
)
YIELD graphName, nodeCount, relationshipCount;

// Step3: 根據年份進行處理
UNWIND [
  {name: '1990s', start: 1990, end: 2000},
  {name: '2000s', start: 2000, end: 2010},
  {name: '2010s', start: 2010, end: 9999}
] AS period

// 建立 Genre 共現圖 (Cypher 投影)
CALL gds.graph.drop("genre-" + period.name, FALSE) YIELD modificationTime
CALL gds.graph.project.cypher(
  'genre-' + period.name,
  'MATCH (g:Genre) RETURN id(g) AS id',
  '
  MATCH (g1:Genre)<-[:HAS_GENRE]-(m:Movie)-[:HAS_GENRE]->(g2:Genre)
  WHERE g1 <> g2 AND m.released >= $start AND m.released < $end
  RETURN id(g1) AS source, id(g2) AS target, count(m) AS weight
  ',
  {parameters: {start: period.start, end: period.end}}
)
YIELD graphName, nodeCount, relationshipCount
CALL gds.louvain.write('genre-' + period.name, {
  writeProperty: 'community_' + period.name
})
YIELD communityCount, modularity

RETURN period.name AS Period, communityCount, modularity;


// [optional] Step 4: 使用 Conductance 評估社群品質


// Step 5: 視覺化類型演化趨勢
MATCH (g:Genre)
WHERE g.community_1990s IS NOT NULL 
  AND g.community_2000s IS NOT NULL 
  AND g.community_2010s IS NOT NULL
RETURN 
  g.name AS Genre,
  g.community_1990s AS Era_1990s,
  g.community_2000s AS Era_2000s,
  g.community_2010s AS Era_2010s,
  CASE 
    WHEN g.community_1990s = g.community_2000s 
     AND g.community_2000s = g.community_2010s 
    THEN 'Stable'
    WHEN g.community_1990s <> g.community_2010s 
    THEN 'Evolved'
    ELSE 'Transitioning'
  END AS EvolutionPattern
ORDER BY Genre;

// Step 6: 識別融合趨勢 (如 Sci-Fi + Action)
MATCH (g1:Genre)<-[:HAS_GENRE]-(m:Movie)-[:HAS_GENRE]->(g2:Genre)
WHERE m.released >= 2010 AND id(g1) < id(g2)
WITH g1, g2, count(m) AS fusionCount
ORDER BY fusionCount DESC
LIMIT 10
RETURN 
  g1.name AS Genre1,
  g2.name AS Genre2,
  fusionCount AS FusionMovies,
  g1.community_2010s = g2.community_2010s AS SameCommunity;

// 清理
CALL gds.graph.list() YIELD graphName
WHERE graphName STARTS WITH 'genre-'
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN collect(dropped);
```

---

### 題目 15: 跨領域知識遷移

**情境**: 識別可以跨類型成功的演員和導演。

#### 解答程式碼:

```cypher
// Step 1: 建立電影人脈圖
CALL gds.graph.project(
  'movie-network',
  'Person',
  {
    ACTED_IN: {orientation: 'UNDIRECTED'},
    DIRECTED: {orientation: 'UNDIRECTED'}
  }
)
YIELD graphName, nodeCount, relationshipCount;


// Step 2: 計算人物的 Degree Centrality（衡量影壇活躍度）
CALL gds.degree.write('movie-network', {
  writeProperty: 'degreeCentrality'
})
YIELD nodePropertiesWritten;

// Step 3: 對每個人計算參與的類型與多樣性
MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(m:Movie)
WITH p, collect(DISTINCT m.genre) AS genres
WITH p, genres, size(genres) AS genreCount
WHERE genreCount >= 2
SET p.genreCount = genreCount, p.genres = genres;

// Step 4: 識別「跨界高手」──同時在多種類型活躍且中心性高的人
MATCH (p:Person)
WHERE p.genreCount >= 3 AND p.degreeCentrality > 5
RETURN 
  p.name AS CrossGenreExpert,
  p.genres AS GenresWorked,
  p.genreCount AS GenreCount,
  round(p.degreeCentrality, 2) AS CentralityScore
ORDER BY GenreCount DESC, CentralityScore DESC
LIMIT 15;

// Step 5: 類型跨越難度矩陣
WITH ['Action', 'Drama', 'Comedy', 'Sci-Fi', 'Thriller', 'Romance'] AS genres
UNWIND genres AS g1
UNWIND genres AS g2
WITH g1, g2 WHERE g1 < g2
MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(:Movie {genre: g1})
WITH g1, g2, collect(DISTINCT p) AS group1
MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(:Movie {genre: g2})
WITH g1, g2, group1, collect(DISTINCT p) AS group2
WITH g1, g2, size([x IN group1 WHERE x IN group2]) AS crossover, size(group1) AS total
RETURN 
  g1 AS FromGenre,
  g2 AS ToGenre,
  crossover AS CrossoverArtists,
  CASE WHEN total > 0 THEN round(toFloat(crossover) / total * 100) / 100 ELSE 0 END AS TransitionRate,
  CASE 
    WHEN toFloat(crossover) / total > 0.5 THEN 'Easy'
    WHEN toFloat(crossover) / total > 0.2 THEN 'Moderate'
    ELSE 'Difficult'
  END AS Difficulty
ORDER BY TransitionRate DESC
LIMIT 20;

// Step 6: 清理暫存圖
CALL gds.graph.drop('movie-network') YIELD graphName
RETURN graphName;

```

---

## 第六部分:性能優化與大規模圖處理

### 題目 16: 記憶體優化的大圖處理

**情境**: 在有限記憶體下處理超大規模圖。

#### 解答程式碼:

```cypher
// Step 1: 使用 Cypher Projection 建立特定子圖
CALL gds.graph.project.cypher(
  'optimized-subgraph',
  'MATCH (p:Person) 
   WHERE size((p)-[:ACTED_IN]->()) >= 3 
   RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE size((p1)-[:ACTED_IN]->()) >= 3 
     AND size((p2)-[:ACTED_IN]->()) >= 3
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount, projectMillis
RETURN 
  graphName,
  nodeCount AS FilteredNodes,
  relationshipCount AS FilteredRelationships,
  projectMillis AS ProjectionTimeMs;

// Step 2: 分割圖為多個社群進行批次處理
CALL gds.louvain.write('optimized-subgraph', {
  writeProperty: 'batchCommunity',
  maxLevels: 10
})
YIELD communityCount, modularity;

MATCH (p:Person)
WHERE p.batchCommunity IS NOT NULL
WITH p.batchCommunity AS community, count(p) AS size
RETURN 
  community AS BatchID,
  size AS CommunitySize
ORDER BY size DESC
LIMIT 10;

// Step 3: 對每個批次分別執行 PageRank
MATCH (p:Person)
WHERE p.batchCommunity IS NOT NULL
WITH DISTINCT p.batchCommunity AS communityId
LIMIT 5
CALL {
  WITH communityId
  MATCH (p:Person {batchCommunity: communityId})
  WITH collect(id(p)) AS communityNodes
  
  CALL gds.graph.project.cypher(
    'batch-' + communityId,
    'MATCH (p:Person) WHERE id(p) IN $nodes RETURN id(p) AS id',
    'MATCH (p1:Person)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2:Person)
     WHERE id(p1) IN $nodes AND id(p2) IN $nodes
     RETURN id(p1) AS source, id(p2) AS target',
    {parameters: {nodes: communityNodes}}
  )
  YIELD graphName
  
  CALL gds.pageRank.write(graphName, {
    writeProperty: 'pagerank_batch',
    maxIterations: 20
  })
  YIELD nodePropertiesWritten, ranIterations
  
  RETURN communityId, nodePropertiesWritten
}
RETURN communityId, nodePropertiesWritten;

// Step 4: 合併批次結果
MATCH (p:Person)
WHERE p.pagerank_batch IS NOT NULL
WITH max(p.pagerank_batch) AS maxPR
MATCH (p:Person)
WHERE p.pagerank_batch IS NOT NULL
SET p.normalizedPR = p.pagerank_batch / maxPR
RETURN 
  p.name AS Person,
  p.batchCommunity AS Batch,
  round(p.pagerank_batch * 1000) / 1000 AS BatchPageRank,
  round(p.normalizedPR * 1000) / 1000 AS NormalizedPR
ORDER BY p.normalizedPR DESC
LIMIT 20;

// Step 5: Stream mode vs Write mode 記憶體比較
// Stream mode (不寫回資料庫)
CALL gds.pageRank.stream('optimized-subgraph', {
  maxIterations: 20
})
YIELD nodeId, score
WITH count(*) AS streamCount
RETURN 'Stream Mode' AS Mode, streamCount AS ProcessedNodes, 'Low Memory' AS MemoryUsage;

// Write mode (寫回資料庫)
CALL gds.pageRank.write('optimized-subgraph', {
  writeProperty: 'pagerank_write',
  maxIterations: 20
})
YIELD nodePropertiesWritten
RETURN 'Write Mode' AS Mode, nodePropertiesWritten AS ProcessedNodes, 'Higher Memory' AS MemoryUsage;

// Step 6: 比較不同投影方式的性能
// Native Projection
CALL gds.graph.project(
  'native-projection',
  'Person',
  {ACTED_IN: {orientation: 'UNDIRECTED'}}
)
YIELD graphName, projectMillis AS nativeTime, nodeCount, relationshipCount
WITH 'Native' AS method, nativeTime, nodeCount, relationshipCount

// Cypher Projection
CALL gds.graph.project.cypher(
  'cypher-projection',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2:Person)
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, projectMillis AS cypherTime, nodeCount AS cypherNodes, relationshipCount AS cypherRels

RETURN 
  method,
  nativeTime AS ProjectionTimeMs,
  nodeCount AS Nodes,
  relationshipCount AS Relationships,
  'Cypher Projection' AS Method2,
  cypherTime AS CypherTimeMs,
  cypherNodes AS CypherNodes,
  cypherRels AS CypherRelationships;

// 清理
CALL gds.graph.list() YIELD graphName
WHERE graphName IN ['optimized-subgraph', 'native-projection', 'cypher-projection'] 
   OR graphName STARTS WITH 'batch-'
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN collect(dropped);
```

---

### 題目 17: 並行演算法調度優化 (unlicenced版本只能用4顆核心)

**情境**: 設計高效的 GDS 演算法執行計畫。

#### 解答程式碼:

```cypher
// Step 1: 建立測試圖
CALL gds.graph.drop("scheduling-network");
CALL gds.graph.project(
  'scheduling-network',
  ['Person', 'Movie'],
  {
      ACTED_IN: {orientation: 'UNDIRECTED'},
      DIRECTED: {orientation: 'UNDIRECTED'}
  }
)
YIELD graphName, nodeCount, relationshipCount;

// Step 2: 分析演算法依賴關係並規劃執行順序
// 階段 1: 獨立演算法 (可並行)
WITH timestamp() AS phase1Start

// 並行任務 1: Degree Centrality
CALL gds.degree.write('scheduling-network', {
  writeProperty: 'degree_sched',
  concurrency: 4
})
YIELD nodePropertiesWritten AS degreeNodes, computeMillis AS degreeTime

WITH phase1Start, degreeNodes, degreeTime

// 並行任務 2: Triangle Count  
CALL gds.triangleCount.write('scheduling-network', {
  writeProperty: 'triangles_sched',
  concurrency: 4
})
YIELD nodePropertiesWritten AS triangleNodes, computeMillis AS triangleTime

RETURN 
  'Phase 1: Independent Algorithms' AS Phase,
  degreeTime AS DegreeTimeMs,
  triangleTime AS TriangleTimeMs,
  'Can run in parallel' AS Note;

// 階段 2: 依賴前階結果的演算法
WITH timestamp() AS phase2Start

// PageRank (獨立)
CALL gds.pageRank.write('scheduling-network', {
  writeProperty: 'pagerank_sched',
  maxIterations: 20,
  concurrency: 4
})
YIELD nodePropertiesWritten, ranIterations, computeMillis AS prTime

WITH phase2Start, prTime

// Louvain (依賴圖結構,但可與 PageRank 並行)
CALL gds.louvain.write('scheduling-network', {
  writeProperty: 'community_sched',
  concurrency: 4
})
YIELD communityCount, modularity, computeMillis AS louvainTime

RETURN 
  'Phase 2: Structure Analysis' AS Phase,
  prTime AS PageRankTimeMs,
  louvainTime AS LouvainTimeMs,
  'Can run in parallel' AS Note;

// 階段 3: 依賴前面結果的演算法
WITH timestamp() AS phase3Start

// Node Similarity (依賴完整圖結構)
CALL gds.nodeSimilarity.write('scheduling-network', {
  writeRelationshipType: 'SIMILAR_SCHED',
  writeProperty: 'similarity',
  concurrency: 4,
  similarityCutoff: 0.1
})
YIELD nodesCompared, relationshipsWritten, computeMillis AS simTime

RETURN 
  'Phase 3: Similarity Computation' AS Phase,
  simTime AS NodeSimilarityTimeMs,
  'Depends on complete graph' AS Note;

// Step 3: 測試不同 concurrency 參數的影響
WITH [1, 2, 4] AS concurrencyLevels
UNWIND concurrencyLevels AS conc
// 在外層取得時間，並傳入子查詢
WITH conc, timestamp() AS startTime
CALL {
  WITH conc
  CALL gds.pageRank.stream('scheduling-network', {
    maxIterations: 20,
    concurrency: conc
  })
  YIELD nodeId, score
  RETURN count(*) AS nodesProcessed
}
WITH conc, startTime, nodesProcessed, timestamp() AS endTime
RETURN 
  conc AS Concurrency,
  (endTime - startTime) AS ExecutionTimeMs,
  nodesProcessed AS NodesProcessed
ORDER BY Concurrency;

// Step 4: 實作結果快取策略
// 快取 PageRank 結果避免重複計算
MATCH (p:Person)
WHERE p.pagerank_sched IS NOT NULL
WITH collect({id: id(p), pr: p.pagerank_sched}) AS cachedResults
WITH cachedResults, size(cachedResults) AS cacheSize

// 模擬需要 PageRank 的後續計算
MATCH (p:Person)
WHERE id(p) IN [r IN cachedResults | r.id][..10]
RETURN 
  p.name AS Person,
  p.pagerank_sched AS CachedPageRank,
  'Retrieved from cache' AS Source
LIMIT 10;

// Step 5: 監控每個步驟的執行時間和記憶體
CALL gds.graph.list('scheduling-network')
YIELD graphName, memoryUsage, sizeInBytes, nodeCount, relationshipCount
RETURN 
  graphName AS Graph,
  memoryUsage AS MemoryUsage,
  sizeInBytes AS SizeBytes,
  nodeCount AS Nodes,
  relationshipCount AS Relationships;

// Step 6: 完整的優化執行計畫
WITH timestamp() AS totalStart

// 並行階段 1
CALL {
  CALL gds.degree.write('scheduling-network', {
    writeProperty: 'degree_final',
    concurrency: 4
  })
  YIELD computeMillis
  RETURN computeMillis AS time1
}

CALL {
  CALL gds.triangleCount.write('scheduling-network', {
    writeProperty: 'triangles_final',
    concurrency: 4
  })
  YIELD computeMillis
  RETURN computeMillis AS time2
}

WITH totalStart, time1, time2

// 並行階段 2
CALL {
  CALL gds.pageRank.write('scheduling-network', {
    writeProperty: 'pagerank_final',
    maxIterations: 20,
    concurrency: 4
  })
  YIELD computeMillis
  RETURN computeMillis AS time3
}

CALL {
  CALL gds.louvain.write('scheduling-network', {
    writeProperty: 'community_final',
    concurrency: 4
  })
  YIELD computeMillis
  RETURN computeMillis AS time4
}

WITH totalStart, time1, time2, time3, time4, timestamp() AS totalEnd

RETURN 
  'Optimized Pipeline' AS Pipeline,
  time1 AS DegreeMs,
  time2 AS TriangleMs,
  time3 AS PageRankMs,
  time4 AS LouvainMs,
  (totalEnd - totalStart) AS TotalElapsedMs,
  'Parallel execution where possible' AS Strategy;

// 清理
CALL gds.graph.drop('scheduling-network');
```
---

### 題目 18: 漸進式圖更新策略 

**情境**: 圖數據持續增長,需要高效更新分析結果。

#### 解答程式碼:

```cypher
// Step 1: 建立基線圖
CALL gds.graph.project(
  'incremental-network',
  ['Person', 'Movie'],
  {ACTED_IN: {orientation: 'UNDIRECTED'}}
)
YIELD graphName, nodeCount, relationshipCount;

// Step 2: 計算基線分析結果
CALL gds.pageRank.write('incremental-network', {
  writeProperty: 'pagerank_baseline',
  maxIterations: 20
})
YIELD nodePropertiesWritten, ranIterations;

CALL gds.louvain.write('incremental-network', {
  writeProperty: 'community_baseline'
})
YIELD communityCount, modularity AS baselineModularity;

// 保存基線統計
MATCH (p:Person)
WHERE p.pagerank_baseline IS NOT NULL
WITH 
  avg(p.pagerank_baseline) AS avgPR,
  stdDev(p.pagerank_baseline) AS stdPR,
  count(p) AS totalNodes
RETURN 
  'Baseline Statistics' AS Stage,
  avgPR AS AveragePageRank,
  stdPR AS StdDevPageRank,
  totalNodes AS TotalNodes;

// Step 3: 模擬新節點和邊的增加 (+10% 數據)
MATCH (existingPerson:Person)
WITH existingPerson, rand() AS r
WHERE r < 0.1
LIMIT toInteger(0.1 * count(existingPerson))
CREATE (newPerson:Person {
  name: 'New_' + existingPerson.name + '_' + toInteger(rand() * 1000),
  isNew: true
})
WITH existingPerson, newPerson
CREATE (newPerson)-[:ACTED_IN]->(newMovie:Movie {
  title: 'New Movie ' + toInteger(rand() * 1000),
  released: 2024,
  isNew: true
})
CREATE (existingPerson)-[:ACTED_IN]->(newMovie);

// Step 4: 識別受影響的節點
MATCH (affected:Person)-[:ACTED_IN]->(:Movie {isNew: true})
SET affected.affected = true
WITH count(DISTINCT affected) AS affectedCount
RETURN 
  'Impact Analysis' AS Stage,
  affectedCount AS AffectedNodes,
  'These nodes need recalculation' AS Note;

// Step 5: 智能更新 - 只對受影響的社群重新計算
MATCH (affected:Person {affected: true})
WITH DISTINCT affected.community_baseline AS affectedCommunity
MATCH (member:Person {community_baseline: affectedCommunity})
WITH collect(id(member)) AS communityNodes, affectedCommunity

CALL gds.graph.project.cypher(
  'affected-community-' + affectedCommunity,
  'MATCH (p:Person) WHERE id(p) IN $nodes RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2:Person)
   WHERE id(p1) IN $nodes AND id(p2) IN $nodes
   RETURN id(p1) AS source, id(p2) AS target',
  {parameters: {nodes: communityNodes}}
)
YIELD graphName, nodeCount

CALL gds.pageRank.write(graphName, {
  writeProperty: 'pagerank_updated',
  maxIterations: 20
})
YIELD nodePropertiesWritten

RETURN 
  affectedCommunity AS Community,
  nodeCount AS NodesRecalculated,
  nodePropertiesWritten AS UpdatedNodes;

// Step 6: Delta-based approach - 追蹤 PageRank 變化
MATCH (p:Person)
WHERE p.pagerank_baseline IS NOT NULL AND p.pagerank_updated IS NOT NULL
WITH p, 
     abs(p.pagerank_updated - p.pagerank_baseline) AS delta,
     ((p.pagerank_updated - p.pagerank_baseline) / p.pagerank_baseline * 100) AS percentChange
WHERE delta > 0.001
SET p.pagerank_delta = delta,
    p.pagerank_change_pct = percentChange
RETURN 
  p.name AS Person,
  round(p.pagerank_baseline * 1000) / 1000 AS BaselinePR,
  round(p.pagerank_updated * 1000) / 1000 AS UpdatedPR,
  round(delta * 1000) / 1000 AS Delta,
  round(percentChange) AS PercentChange
ORDER BY delta DESC
LIMIT 15;

// Step 7: 設計觸發條件 - 決定何時全圖重算
MATCH (p:Person)
WHERE p.pagerank_delta IS NOT NULL
WITH 
  count(p) AS nodesChanged,
  avg(p.pagerank_delta) AS avgDelta,
  max(p.pagerank_delta) AS maxDelta
MATCH (all:Person)
WITH nodesChanged, avgDelta, maxDelta, count(all) AS totalNodes
WITH 
  nodesChanged,
  avgDelta,
  maxDelta,
  totalNodes,
  toFloat(nodesChanged) / totalNodes AS changeRatio,
  CASE 
    WHEN toFloat(nodesChanged) / totalNodes > 0.3 THEN 'FULL_RECALC'
    WHEN avgDelta > 0.01 THEN 'FULL_RECALC'
    WHEN maxDelta > 0.1 THEN 'PARTIAL_RECALC'
    ELSE 'INCREMENTAL_OK'
  END AS recommendation
RETURN 
  'Update Strategy Decision' AS Analysis,
  nodesChanged AS ChangedNodes,
  totalNodes AS TotalNodes,
  round(changeRatio * 100) AS ChangePercentage,
  round(avgDelta * 1000) / 1000 AS AvgDelta,
  round(maxDelta * 1000) / 1000 AS MaxDelta,
  recommendation AS RecommendedStrategy;

// Step 8: 性能比較
// 增量更新時間 (已執行)
WITH timestamp() AS incrementalTime
MATCH (p:Person {affected: true})
WITH count(p) AS incrementalNodes, incrementalTime

// 模擬全圖重算時間
WITH incrementalNodes, timestamp() AS fullRecalcStart
CALL gds.pageRank.stream('incremental-network', {
  maxIterations: 20
})
YIELD nodeId, score
WITH incrementalNodes, count(*) AS fullRecalcNodes, timestamp() - fullRecalcStart AS fullRecalcTime

RETURN 
  'Performance Comparison' AS Metric,
  incrementalNodes AS IncrementalNodes,
  fullRecalcNodes AS FullRecalcNodes,
  fullRecalcTime AS FullRecalcTimeMs,
  'Incremental is much faster' AS Conclusion;

// 清理
CALL gds.graph.list() YIELD graphName
WHERE graphName = 'incremental-network' OR graphName STARTS WITH 'affected-community-'
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN collect(dropped);
```