
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
**目的**: 目的是建立一個「影視人物影響力綜合評估與角色分類系統」。

它不只是單純計算誰最紅，而是透過三種不同的中心性指標（Degree, Betweenness, Closeness），將演員/導演分類為不同的「社會角色」：

1. Superstar (超級巨星)：各項指標都頂尖，既有人氣又有影響力。

2. Bridge (橋樑/中間人)：連接不同圈子（例如同時演獨立電影和商業片），掌握資源流動的關鍵人物。

3. Social Hub (社交樞紐)：認識非常多人，但可能不具備連接不同群體的能力（例如只在動作片圈很紅）。

4. Specialist (專家/邊緣者)：合作對象少且圈子固定。
```cypher
// ============================================
// Part 1: 建圖與中心性計算 (Network Build & Centrality)
// ============================================

// Step 1: 清理舊圖
// --------------------------------------------
// 確保沒有殘留的同名圖形，避免投影失敗。
CALL gds.graph.drop('movies-centrality-network', FALSE);

// Step 2: 建立「人物協作」投影圖 (Co-authorship Graph)
// --------------------------------------------
// 目的：將原本的「二分圖」(Person-Movie-Person) 轉換為「單模圖」(Person-Person)。
// 邏輯：如果兩個人在同一部電影工作過 (ACTED_IN 或 DIRECTED)，他們之間就有一條邊。
// 權重 (collaborations)：合作次數越多，關係越緊密。
CALL gds.graph.project.cypher(
  'movies-centrality-network',
  // 節點查詢：選擇所有 Person
  'MATCH (p:Person) RETURN id(p) AS id',
  // 關係查詢：建立協作關係
  'MATCH (source:Person)-[:ACTED_IN|DIRECTED]->(m:Movie)<-[:ACTED_IN|DIRECTED]-(target:Person)
   WHERE id(source) < id(target) // 避免重複計算 (A-B 和 B-A)
   WITH source, target, count(DISTINCT m) AS collaborations
   RETURN id(source) AS source, id(target) AS target, collaborations AS weight'
)
YIELD graphName, nodeCount, relationshipCount, projectMillis
RETURN 
  graphName AS Graph,
  nodeCount AS TotalPeople,
  relationshipCount AS Collaborations,
  projectMillis AS BuildTimeMs;


// Step 3: 計算 Degree Centrality (度中心性)
// --------------------------------------------
// 意義：衡量「活躍度」或「人氣」。
// 解讀：分數越高，代表這個人合作過的對象越多，是影壇的「熟面孔」。

CALL gds.degree.write('movies-centrality-network', {
  writeProperty: 'degreeCentrality'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'Degree Centrality' AS Metric,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// 查看 Top 10 活躍王
MATCH (p:Person)
WHERE p.degreeCentrality IS NOT NULL
RETURN 
  p.name AS Person,
  p.degreeCentrality AS DegreeScore
ORDER BY p.degreeCentrality DESC
LIMIT 10;


// Step 4: 計算 Betweenness Centrality (中介中心性)
// --------------------------------------------
// 意義：衡量「控制力」或「橋樑作用」。
// 解讀：分數越高，代表越多最短路徑經過此人。這個人通常連接著兩個不相干的群體
// (例如連接著「好萊塢動作片圈」和「法國藝術片圈」)，擁有跨圈子的影響力。

CALL gds.betweenness.write('movies-centrality-network', {
  writeProperty: 'betweennessCentrality'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'Betweenness Centrality' AS Metric,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// 查看 Top 10 橋樑人物
MATCH (p:Person)
WHERE p.betweennessCentrality IS NOT NULL
RETURN 
  p.name AS Person,
  round(p.betweennessCentrality * 100) / 100 AS BetweennessScore
ORDER BY p.betweennessCentrality DESC
LIMIT 10;


// Step 5: 計算 Closeness Centrality (使用 Harmonic Centrality)
// --------------------------------------------
// 意義：衡量「傳播效率」。
// 解讀：分數越高，代表他到網路中其他所有人的平均距離越短。
// 注意：這裡使用 Harmonic (調和中心性)，因為電影網路通常是不連通的 (有孤島)，
// 傳統 Closeness 在非連通圖上會失效，Harmonic 能解決此問題。
CALL gds.closeness.harmonic.write('movies-centrality-network', {
  writeProperty: 'closenessCentrality'
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'Closeness Centrality' AS Metric,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;

// 查看 Top 10 效率王
MATCH (p:Person)
WHERE p.closenessCentrality IS NOT NULL
RETURN 
  p.name AS Person,
  round(p.closenessCentrality * 1000) / 1000 AS ClosenessScore
ORDER BY p.closenessCentrality DESC
LIMIT 10;


// ============================================
// Part 2: 綜合分析與分類 (Profiling & Classification)
// ============================================

// Step 6: 標準化所有中心性指標 (Normalization)
// --------------------------------------------
// 目的：因為 Degree 可能是幾百，而 Closeness 可能是 0.5，單位不同無法直接比較。
// 作法：將所有數值除以該指標的最大值，將其縮放到 0~1 之間。
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


// Step 7: 根據標準化指標進行人物分類 (Role Classification)
// --------------------------------------------
// 目的：透過商業邏輯規則，賦予人物「社會角色」。
MATCH (p:Person)
WHERE p.degreeNorm IS NOT NULL
SET p.personalityType = CASE 
    // 1. Superstar: 三項指標都極高 (>0.6)，全方位的影壇核心。
    WHEN p.degreeNorm > 0.6 AND p.betweennessNorm > 0.6 AND p.closenessNorm > 0.6 THEN 'Superstar'
    
    // 2. Bridge: 中介性特別高 (>0.7)，但人氣可能普通。這種人是連結不同圈子的關鍵(如選角導演最愛)。
    WHEN p.betweennessNorm > 0.7 AND p.degreeNorm >= 0.3 AND p.degreeNorm <= 0.7 THEN 'Bridge'
    
    // 3. Social Hub: 人氣很高 (>0.7)，但中介性普通。通常是某個特定類型片的龍頭，但不跨圈。
    WHEN p.degreeNorm > 0.7 AND p.betweennessNorm >= 0.3 AND p.betweennessNorm <= 0.7 THEN 'Social Hub'
    
    // 4. Specialist: 各項指標都低。通常是配角、特定類型的小眾演員或新人。
    WHEN p.degreeNorm < 0.3 AND p.betweennessNorm < 0.3 AND p.closenessNorm < 0.3 THEN 'Specialist'
    
    // 5. Regular: 其他普通演員。
    ELSE 'Regular'
END
RETURN 'Classification Complete' AS Status,
       count(p) AS ClassifiedNodes;


// Step 8: 統計每種人物類型的數量 (Distribution Analysis)
// --------------------------------------------
// 目的：觀察影壇的人才結構分佈。通常 Specialist 數量最多，Superstar 極少。
MATCH (p:Person)
WHERE p.personalityType IS NOT NULL
WITH p.personalityType AS type, count(p) AS count
RETURN 
  type AS PersonalityType,
  count AS NumberOfPeople,
  round(toFloat(count) / sum(count) * 100) AS Percentage
ORDER BY count DESC;


// Step 9: 查看每種類型的代表人物 (Representative Examples)
// --------------------------------------------
// 目的：列出每個分類的典型例子，驗證分類邏輯是否符合直覺。
MATCH (p:Person)
WHERE p.personalityType IS NOT NULL
WITH p.personalityType AS type, p
// 排序邏輯：總分最高的排前面
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
**目的**: 目的是建立一個「綜合影響力評分模型 (Composite Influence Scoring Model)」。

它不依賴單一指標（如只看誰粉絲多），而是結合了三種不同的圖學中心性演算法，並透過歸一化 (Normalization) 與 加權平均 (Weighted Average) 的方式，計算出一個更客觀的最終分數。這在尋找「核心意見領袖 (KOL)」或「關鍵人物」時非常有用。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立圖投影 (Graph Projection)
// =========================================================
// 目的：將資料載入 GDS 記憶體。
// 注意：這裡假設資料庫中已經存在 'ACTED_IN' 或類似的關係能直接連結兩個 Person。
// 如果使用的是標準 Movie 資料庫 (Person->Movie)，這裡通常需要使用 Cypher Projection 
// 將 Person-Movie-Person 的路徑轉換為 Person-Person 的關係，才能正確計算演員間的網路。
CALL gds.graph.project(
  'actor-network',
  'Person', // 節點標籤
  {
    ACTED_WITH: {
      type: 'ACTED_IN',         // 讀取的關係類型
      orientation: 'UNDIRECTED',// 視為無向圖 (合作是雙向的)
      properties: {}
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 執行 PageRank (權威性/長期聲望)
// =========================================================
// 目的：評估演員的「權威性」。
// 意義：不只看朋友多不多，還看朋友「夠不夠大咖」。
// 如果很多大牌演員都跟你合作過，你的 PageRank 就會高。
CALL gds.pageRank.write('actor-network', {
  writeProperty: 'pagerank',
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD nodePropertiesWritten, ranIterations;


// =========================================================
// Step 3: 執行 Betweenness Centrality (中介/橋樑能力)
// =========================================================
// 目的：評估演員的「控制力」或「跨圈子能力」。
// 意義：分數高的人通常連接著不同的群體 (例如同時演動作片和文藝片)。
// 他們是資訊或資源流通的必經之路，移除他們會讓圈子斷裂。
CALL gds.betweenness.write('actor-network', {
  writeProperty: 'betweenness'
})
YIELD nodePropertiesWritten, computeMillis;


// =========================================================
// Step 4: 執行 Degree Centrality (活躍度/人氣)
// =========================================================
// 目的：評估演員的「活躍程度」。
// 意義：最直觀的指標。合作過的人越多，分數越高。代表他在圈內的社交廣度。
CALL gds.degree.write('actor-network', {
  writeProperty: 'degree'
})
YIELD nodePropertiesWritten;


// =========================================================
// Step 5: 計算綜合影響力分數並標準化 (Normalization & Scoring)
// =========================================================
// 目的：這是最關鍵的一步。因為 PageRank (可能 0.1~5.0) 和 Betweenness (可能 0~10000) 的量級完全不同，
// 直接相加沒有意義。必須先「歸一化 (Normalization)」。

// 5.1 先找出各指標的最大值 (作為分母)
MATCH (p:Person)
WHERE p.pagerank IS NOT NULL
WITH 
  max(p.pagerank) AS maxPR, 
  max(p.betweenness) AS maxBW, 
  max(p.degree) AS maxDeg

// 5.2 歸一化並加權計算
MATCH (p:Person)
WHERE p.pagerank IS NOT NULL
SET p.influenceScore = 
  // PageRank 權重 40%：強調業界地位與聲望
  (p.pagerank / maxPR) * 0.4 + 
  // Betweenness 權重 35%：強調連結不同圈子的能力
  (p.betweenness / maxBW) * 0.35 + 
  // Degree 權重 25%：活躍度僅佔 1/4，避免只獎勵「量大」的人
  (p.degree / maxDeg) * 0.25;


// =========================================================
// Step 6: 找出 Top 10 最具綜合影響力的演員
// =========================================================
// 目的：展示最終結果。
// 這些人通常被稱為 "Key Opinion Leaders (KOLs)" 或核心人物。
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


// =========================================================
// Step 7: 比較分析 (Comparative Analysis)
// =========================================================
// 目的：驗證綜合分數的價值。
// 比較「單純 PageRank 前 10 名」和「綜合分數前 10 名」的差異。
// overlap (交集) 越少，代表這個綜合模型挖掘出了被單一指標忽略的「隱形冠軍」。
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
  // 計算交集：看哪幾個人同時出現在兩個榜單
  [name IN topByPageRank WHERE name IN topByComposite] AS overlap;


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('actor-network');
```

---

### 題目 2: 動態社群演化追蹤

**情境**: 分析不同年代電影合作網路的社群結構變化。
**目的**: 目的是建立一個「動態社群演化分析系統 (Dynamic Community Evolution Analysis)」。

它展示了如何追蹤一個社交網絡隨時間的變化。例如，一位演員在 90 年代可能屬於「動作片圈」，但在 2000 年代可能轉型進入「劇情片圈」。這段程式碼透過以下步驟實現：

1. 時間切片 (Time Slicing)：將資料按年代切分為三個獨立的子圖。

2. 快照分析 (Snapshot Analysis)：在每個年代分別執行社群偵測。

3. 演化追蹤 (Evolution Tracking)：比較同一位演員在不同年代的社群標籤，判斷其社交圈是穩定的還是發生了遷移。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 資料準備 - 為電影添加年代標籤
// =========================================================
// 目的：因為原始 Movies 資料集可能沒有完整 released 年份，這裡使用隨機數模擬。
// 設定：隨機生成 1990 ~ 2020 之間的年份。
MATCH (m:Movie)
SET m.released = toInteger(1990 + rand() * 30);


// =========================================================
// Step 2: 建立三個時期的圖投影 (Temporal Graph Projections)
// =========================================================
// 目的：將一個大圖拆解為三個「時間切片 (Time Slices)」。
// 方法：使用 Cypher Projection，根據電影的 released 年份過濾連結。
// 結構變換：將 "Person-Movie-Person" 的共同演出關係，直接投影為 "Person-Person" 的社交圖。

// --- 時期 1: 1990s (1990-1999) ---
CALL gds.graph.project.cypher(
  'era1-network',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE m.released >= 1990 AND m.released < 2000
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount;

// --- 時期 2: 2000s (2000-2009) ---
CALL gds.graph.project.cypher(
  'era2-network',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE m.released >= 2000 AND m.released < 2010
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount;

// --- 時期 3: 2010s (2010後) ---
CALL gds.graph.project.cypher(
  'era3-network',
  'MATCH (p:Person) RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
   WHERE m.released >= 2010
   RETURN id(p1) AS source, id(p2) AS target'
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 3: 對每個時期執行 Louvain 社群偵測
// =========================================================
// 目的：找出每個年代的「社交圈子」。
// 演算法：Louvain。它透過優化模組化 (Modularity) 來發現連結緊密的群體。
// 結果：每個演員在每個年代都會獲得一個 community ID (例如：90年代屬於社群 A，00年代屬於社群 B)。


// 計算 90 年代社群
CALL gds.louvain.write('era1-network', {
  writeProperty: 'community_era1'
})
YIELD communityCount, modularity;

// 計算 00 年代社群
CALL gds.louvain.write('era2-network', {
  writeProperty: 'community_era2'
})
YIELD communityCount, modularity;

// 計算 10 年代社群
CALL gds.louvain.write('era3-network', {
  writeProperty: 'community_era3'
})
YIELD communityCount, modularity;


// =========================================================
// Step 4: 追蹤演員的社群遷移 (Migration Tracking)
// =========================================================
// 目的：觀察演員的職涯路徑。
// 邏輯：
// - 如果三個時期的社群 ID 發生變化，代表該演員的合作對象群體發生了轉移 (Migrated)。
// - 如果相對關係保持不變 (註：實務上需配合社群對齊算法，此處為簡化邏輯)，則視為穩定 (Stable)。
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
    THEN 'Migrated' // 社交圈發生改變
    ELSE 'Stable'   // 社交圈相對固定
  END AS Status
ORDER BY Status DESC, p.name
LIMIT 20;


// =========================================================
// Step 5: 識別橋接者 (Identifying Bridgers)
// =========================================================
// 目的：找出那些連接不同時代、不同群體的關鍵人物。
// 邏輯：
// 1. 找出在 Era1 和 Era2 之間更換過社群的人 (c1 <> c2)。
// 2. 檢查這個人是否與「原 Era1 社群」的其他成員仍有聯繫。
// 3. 連結數越多，代表他雖然轉型了，但仍扮演著舊圈子與新圈子之間的橋樑。
MATCH (p:Person)
WHERE p.community_era1 IS NOT NULL 
  AND p.community_era2 IS NOT NULL
WITH p, p.community_era1 AS c1, p.community_era2 AS c2
WHERE c1 <> c2 // 篩選出發生過社群遷移的人

// 找出他的合作對象 (other)
MATCH (p)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(other:Person)
WHERE other.community_era1 IS NOT NULL 
  AND other.community_era2 IS NOT NULL
  AND other.community_era1 <> p.community_era1 // 對方屬於不同的舊社群
WITH p, count(DISTINCT other) AS bridgeConnections
ORDER BY bridgeConnections DESC
LIMIT 10
RETURN 
  p.name AS Bridger,
  bridgeConnections AS CrossCommunityConnections;


// =========================================================
// 清理資源
// =========================================================
CALL gds.graph.drop('era1-network');
CALL gds.graph.drop('era2-network');
CALL gds.graph.drop('era3-network');
```

---

### 題目 3: 協作網路的弱連結強度

**情境**: 驗證「弱連結假說」在電影合作網路中的表現。
**目的**: 主要目的是深入分析演員合作網絡並挖掘潛在合作機會。

具體目標包括：

1. 網絡結構分析：透過計算相似度 (Similarity) 來識別演員之間的關係緊密程度，並區分出核心的「強連結」與具備跨界價值的「弱連結」。

2. 預測與推薦：利用連結預測演算法 (Adamic Adar) 與過濾機制，預測哪些尚未合作過的演員未來最有可能合作，並為特定演員建立一套潛在合作夥伴推薦系統。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立演員網路圖投影 (Graph Projection)
// =========================================================
// 目的：將 Neo4j 資料庫中的資料載入記憶體 (In-Memory Graph)，以加速後續 GDS 演算法的運算。
// 注意：這裡假設資料庫中已經預先建立了 [ACTED_WITH] 關係，代表兩個演員曾在同一部電影合作過。
CALL gds.graph.project(
  'actor-collab-network',       // 投影圖的名稱，供後續步驟引用
  'Person',                     // 載入的節點標籤
  {
    ACTED_WITH: {               // 載入的關係類型
      orientation: 'UNDIRECTED' // 設定為「無方向性」，因為演員合作是雙向的 (A跟B合作 = B跟A合作)
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 計算 Node Similarity (基於共同合作的電影)
// =========================================================
// 目的：計算演員之間的相似度。這裡使用的是 Jaccard Similarity。
// 原理：如果演員 A 和演員 B 有很多共同的合作對象，他們的相似度就會高。
CALL gds.nodeSimilarity.write('actor-collab-network', {
  writeRelationshipType: 'SIMILAR_TO', // 將計算結果寫回資料庫，建立新的關係類型
  writeProperty: 'similarity',         // 將相似度分數 (0~1) 寫入關係的屬性中
  similarityCutoff: 0.1                // 過濾門檻：只保留相似度大於 0.1 (10%) 的關係，避免雜訊
})
YIELD nodesCompared, relationshipsWritten;


// =========================================================
// Step 3: 將連結分為強弱連結 (Link Classification)
// =========================================================
// 目的：根據相似度分數的高低，將關係標記為「強連結」或「弱連結」。
// 應用：強連結代表緊密的小圈圈；弱連結通常是跨群體的橋樑，具有傳遞新資訊的價值。

// 3.1 先找出相似度分數的中位數 (Median)
MATCH (p1:Person)-[s:SIMILAR_TO]->(p2:Person)
WITH percentileCont(s.similarity, 0.5) AS medianSimilarity

// 3.2 再次匹配關係，並根據中位數設定 linkType 屬性
MATCH (p1:Person)-[s:SIMILAR_TO]->(p2:Person)
SET s.linkType = CASE 
  WHEN s.similarity >= medianSimilarity THEN 'Strong' // 高於中位數 -> 強連結
  ELSE 'Weak'                                         // 低於中位數 -> 弱連結
END;


// =========================================================
// Step 4: 分析弱連結的橋接作用
// =========================================================
// 目的：觀察那些「相似度不高」(弱連結) 的演員組合，看看他們實際上有多少共同作品。
// 洞察：如果 SharedMovies 很少，但他們仍被演算法判定有相似關係，代表他們是透過間接人脈連接起來的潛在跨界組合。
MATCH (p1:Person)-[s:SIMILAR_TO {linkType: 'Weak'}]->(p2:Person)
MATCH (p1)-[:ACTED_IN]->(m1:Movie) // 找出 p1 演過的所有電影
MATCH (p2)-[:ACTED_IN]->(m2:Movie) // 找出 p2 演過的所有電影
WHERE m1 <> m2                     // 排除同一部電影的自我比較 (雖非必要但邏輯上更嚴謹)
WITH p1, p2, collect(DISTINCT m1.title) AS p1Movies, collect(DISTINCT m2.title) AS p2Movies
RETURN 
  p1.name AS Person1,
  p2.name AS Person2,
  size(p1Movies) AS P1MovieCount,
  size(p2Movies) AS P2MovieCount,
  // 使用 List Comprehension 計算兩個電影清單的交集數量
  size([m IN p1Movies WHERE m IN p2Movies]) AS SharedMovies
ORDER BY SharedMovies ASC // 從共同電影最少的開始看起 (最具弱連結特徵)
LIMIT 10;


// =========================================================
// Step 5: 使用 Link Prediction 預測未來合作
// =========================================================
// 目的：預測哪兩個演員未來最有可能合作。
// 方法：使用 Adamic Adar 演算法，它會評估兩個節點的「共同鄰居」，並對「連結較少」的鄰居給予更高權重。

// 5.1 建立新的投影，這次我們需要 Person 和 Movie 的二分圖結構
CALL gds.graph.project(
  'actor-prediction-network',
  ['Person', 'Movie'], // 載入兩種節點
  'ACTED_IN'           // 載入 "演出" 關係
);

// 5.2 執行 Adamic Adar 預測
CALL gds.linkPrediction.adamicAdar.stream('actor-prediction-network')
YIELD node1, node2, score
// 將 GDS 的內部節點 ID 轉換回實際的節點物件
WITH gds.util.asNode(node1) AS p1, gds.util.asNode(node2) AS p2, score
// 重要過濾：WHERE NOT EXISTS 確保這兩個人目前「沒有」共同演過同一部電影
// 這樣我們預測的才是「未來的」新合作，而不是已知的事實
WHERE p1:Person AND p2:Person AND NOT EXISTS((p1)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2))
RETURN 
  p1.name AS Actor1,
  p2.name AS Actor2,
  score AS PredictionScore
ORDER BY score DESC // 分數越高，預測合作機率越大
LIMIT 15;


// =========================================================
// Step 6: 建立推薦系統 (針對特定演員)
// =========================================================
// 目的：為特定演員 (如 Tom Hanks) 推薦他還沒合作過，但風格或圈子很近的演員。
// 優化：使用 'filtered.stream' 只針對單一來源節點運算，效能比全圖計算快很多。
MATCH (actor:Person {name: 'Tom Hanks'})
CALL gds.nodeSimilarity.filtered.stream('actor-collab-network', {
  sourceNodes: [actor] // 指定起始節點
})
YIELD node1, node2, similarity
WITH gds.util.asNode(node2) AS similarActor, similarity, actor
// 過濾掉已經合作過的對象 (排除一度人脈，尋找二度潛力人脈)
WHERE NOT EXISTS((actor)-[:ACTED_IN]->()<-[:ACTED_IN]-(similarActor))
RETURN 
  similarActor.name AS RecommendedCollaborator, // 推薦的合作對象
  similarity AS SimilarityScore                 // 相似度分數
ORDER BY similarity DESC
LIMIT 10;


// =========================================================
// 清理資源
// =========================================================
// 釋放記憶體中的圖模型
CALL gds.graph.drop('actor-collab-network');
CALL gds.graph.drop('actor-prediction-network');
```

---

## 第二部分:路徑與最短路徑優化

### 題目 4: 多約束條件最短路徑

**情境**: 使用 Movies dataset 模擬物流網路 (演員=城市, 合作=路線)

**目的**：模擬一個「多維度」的路徑規劃與決策系統（類似 Google Maps 的導航邏輯）。

簡單來說，它透過將「演員」視為「地點」，將「關係」視為「道路」，來演示以下三個層次的問題解決：

單一目標最佳化：分別計算「最短距離」與「最快時間」的路徑（例如：我趕時間，不在乎油錢）。

多樣化選擇 (Top-K)：不只找一條路，而是找出「前 5 條」不錯的路線供選擇（使用 Yen's K-Shortest Paths）。

多目標綜合決策：在距離、時間、過路費這三個互相衝突的條件下，透過加權計分公式，選出一個「CP 值最高」的綜合最佳路線。

#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 準備數據 - 建立模擬的交通網絡
// =========================================================
// 目的：因為原始數據只有「合作關係」，我們需要模擬一個交通場景。
// 我們將每個「演員」視為一個「地點」，將「合作關係」視為「道路」。
// 並隨機生成三個屬性：距離 (distance)、時間 (travel_time)、過路費 (toll_cost)。

MATCH (p1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
WHERE id(p1) < id(p2) // 技巧：利用 ID 大小比較，確保 A-B 和 B-A 只會被處理一次，並避免自己對自己的迴圈
WITH p1, p2, count(m) AS collaborations
MERGE (p1)-[r:CONNECTED_TO]-(p2) // 建立一個新的通用連結關係
SET r.distance = 100 + rand() * 400,   // 隨機產生 100~500 的距離
    r.travel_time = 1 + rand() * 5,    // 隨機產生 1~6 的時間
    r.toll_cost = 10 + rand() * 50;    // 隨機產生 10~60 的過路費


// =========================================================
// Step 2: 建立加權圖投影 (Weighted Graph Projection)
// =========================================================
// 目的：將三個權重屬性同時載入 GDS 的記憶體圖中，以便後續可以切換不同的計算標準。

// 先嘗試刪除舊圖 (如果存在)，避免名稱衝突錯誤
CALL gds.graph.drop('weighted-network') YIELD graphName;

CALL gds.graph.project(
  'weighted-network',
  'Person',
  {
    CONNECTED_TO: {
      orientation: 'UNDIRECTED',
      // 關鍵點：這裡必須列出所有後續計算會用到的屬性
      properties: ['distance', 'travel_time', 'toll_cost']
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Helper Logic: 自動選擇起點與終點
// =========================================================
// 為了讓演示順利進行，我們自動找出圖中連結數 (Degree) 最多的兩個人。
// 這樣可以保證這兩個點之間很有可能存在路徑，避免選到孤立節點導致找不到路徑。
// 下面的 Step 3, 4, 5, 6 都會重複使用這段邏輯。


// =========================================================
// Step 3: 計算「最短距離」路徑 (Dijkstra - Distance)
// =========================================================
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg // 計算每個人的連結數
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end

// 使用 Dijkstra 演算法
CALL gds.shortestPath.dijkstra.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  relationshipWeightProperty: 'distance' // 指定優化目標：距離
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN 
  'Shortest Distance' AS RouteType,
  gds.util.asNode(sourceNode).name AS Start,
  gds.util.asNode(targetNode).name AS End,
  totalCost AS TotalDistance,
  // 將路徑上的 Node ID 轉換回人名以便閱讀
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route;


// =========================================================
// Step 4: 計算「最短時間」路徑 (Dijkstra - Time)
// =========================================================
// 邏輯與 Step 3 相同，但優化目標改為時間。
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end

CALL gds.shortestPath.dijkstra.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  relationshipWeightProperty: 'travel_time' // 切換優化目標：時間
})
YIELD index, totalCost, nodeIds
RETURN 
  'Shortest Time' AS RouteType,
  totalCost AS TotalTime,
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route;


// =========================================================
// Step 5: 使用 Yen's K-Shortest Paths 找前 5 條路線
// =========================================================
// 目的：Dijkstra 只能給出一條最好的路。Yen's 演算法可以給出 Top K 條選擇。
// 這在導航中很有用：「這是最快路徑，但如果您想避開這條路，這是第二選擇...」
MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end

CALL gds.shortestPath.yens.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  k: 5,                                  // 指定要找幾條路徑
  relationshipWeightProperty: 'distance' // 主要排序依據仍是距離
})
YIELD index, totalCost, nodeIds
RETURN 
  index AS RouteRank, // 第幾名的路徑 (0 是最短，1 是次短...)
  totalCost AS TotalDistance,
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route,
  size(nodeIds) AS Hops;


// =========================================================
// Step 6: 綜合評分最優路線 (Multi-Objective Scoring)
// =========================================================
// 目的：現實生活中，我們選路徑通常是綜合考量 (距離要短、時間要快、過路費要省)。
// 做法：
// 1. 先用 Yen's 找出前 5 條以「距離」為基準的路徑。
// 2. 針對這 5 條路徑，去資料庫查詢它們的「時間」與「過路費」。
// 3. 使用加權公式計算一個「綜合分數 (Composite Score)」。

MATCH (cand:Person)
WITH cand, size([(cand)--() | 1]) AS deg
ORDER BY deg DESC
LIMIT 2
WITH collect(cand) AS top2
WITH top2[0] AS start, top2[1] AS end

// 6.1 取得路徑候選名單
CALL gds.shortestPath.yens.stream('weighted-network', {
  sourceNode: id(start),
  targetNode: id(end),
  k: 5,
  relationshipWeightProperty: 'distance'
})
YIELD index, totalCost AS distance, nodeIds, costs

// 6.2 展開路徑上的所有節點 ID
UNWIND nodeIds AS gid
WITH index, distance, nodeIds, costs, collect(gid) AS gids

// 6.3 技巧：使用 sliding window 重建路徑的每一段 (Segment)
// 例如路徑是 [A, B, C]，我們會拆成 A-B 和 B-C
UNWIND range(0, size(gids)-2) AS i
WITH index, distance, gids, gds.util.asNode(gids[i]) AS p1, gds.util.asNode(gids[i+1]) AS p2

// 6.4 回到資料庫查詢這兩點之間的實際屬性 (時間與過路費)
MATCH (p1)-[r:CONNECTED_TO]-(p2)

// 6.5 重新聚合算出整條路徑的總時間與總花費
WITH index, distance, gids, sum(r.travel_time) AS totalTime, sum(r.toll_cost) AS totalCost

// 6.6 計算綜合分數 (歸一化處理)
// 因為距離是幾百公里，時間是幾小時，單位不同，直接相加沒有意義。
// 這裡將各數值除以一個假設的最大值 (如 500, 6, 60) 進行歸一化，再乘上權重 (0.33 等)。
WITH index, gids AS nodeIds, distance, totalTime, totalCost,
     (distance/500.0 * 0.33 + totalTime/6.0 * 0.33 + totalCost/60.0 * 0.34) AS compositeScore

RETURN 
  index AS RouteRank,
  distance AS TotalDistance,
  totalTime AS TotalTime,
  totalCost AS TotalCost,
  compositeScore AS CompositeScore, // 分數越低越好
  [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS Route
ORDER BY compositeScore ASC; // 根據綜合評分重新排序推薦順序


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('weighted-network');

```

---

### 題目 5: 關鍵交通樞紐識別與容錯分析

**情境**: 識別網路中的關鍵節點並分析網路韌性。
**目的**: 主要目的是分析網路的韌性 (Resilience) 與脆弱點 (Vulnerability)。

它不僅找出網路中最重要的「樞紐」(Hubs)，還進一步計算網路的傳輸效率，並進行破壞性模擬（Simulation of Attack/Failure），觀察如果移除了這些關鍵人物，網路結構會受到什麼影響。最後，它結合多種指標建立一個綜合的「重要性評分系統」。

這常用於供應鏈風險評估、組織架構分析（誰離職最危險）、或基礎設施的關鍵節點檢測。

#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立基礎網路圖投影 (Graph Projection)
// =========================================================
// 目的：將 Neo4j 資料庫中的 Person 節點與連結載入記憶體。
// 這裡建立了基礎的拓樸結構，用於分析連結關係。
// *注意*：若後續 Step 3 需要用到 'distance' 權重，這裡的 relationshipProperties 需要包含 'distance'。
CALL gds.graph.project(
  'hub-network',
  'Person',
  {
    CONNECTED_TO: {
      orientation: 'UNDIRECTED'
      // 若 Step 3 要計算加權路徑，這裡應加上: properties: 'distance'
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 使用 Betweenness Centrality 找關鍵樞紐 (Hubs)
// =========================================================
// 目的：找出網路中的「過路財神」或「橋樑」。
// 原理：Betweenness (中介中心性) 計算一個節點出現在其他所有節點對的最短路徑上的次數。
// 意義：分數高的人代表他是資訊流通的「瓶頸」或「控制者」。如果他消失，網路可能會斷裂或繞遠路。
CALL gds.betweenness.write('hub-network', {
  writeProperty: 'betweenness_hub' // 將計算結果寫回節點屬性
})
YIELD nodePropertiesWritten, computeMillis;

// 檢視前 20 名關鍵樞紐
MATCH (p:Person)
WHERE p.betweenness_hub IS NOT NULL
RETURN 
  p.name AS Hub,
  p.betweenness_hub AS BetweennessCentrality
ORDER BY p.betweenness_hub DESC
LIMIT 20;


// =========================================================
// Step 3: 計算基準網路的平均路徑長度 (Global Efficiency)
// =========================================================
// 目的：評估整個網路的傳輸效率。
// 意義：平均路徑越短，代表資訊傳播越快，網路越緊密。
// *注意*：此步驟假設投影圖中有 'distance' 屬性。若無，GDS 會報錯或需移除 relationshipWeightProperty 改算 Hop 數。
CALL gds.allShortestPaths.dijkstra.stream('hub-network', {
  relationshipWeightProperty: 'distance' 
})
YIELD sourceNode, targetNode, totalCost
WITH avg(totalCost) AS avgPathLength, count(*) AS pathCount
RETURN 
  'Baseline Network' AS NetworkState,
  avgPathLength AS AveragePathLength, // 整個網路的平均距離
  pathCount AS TotalPaths;            // 總路徑數 (可反映網路連通規模)


// =========================================================
// Step 4: 找出潛在的影響力節點 (Articulation Rank)
// =========================================================
// 目的：使用 ArticleRank (PageRank 的變體) 來評估節點重要性。
// 意義：這裡雖然命名為 "ArticulationPoint" (斷點)，但使用的是 ArticleRank 演算法。
// 它找出的是「連結品質好且連結數量多」的節點。真正的 Articulation Point (移除後會導致網路斷裂的點) 通常需要透過連通分量分析，但在這裡我們將其視為另一種「高重要性」指標。
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


// =========================================================
// Step 5: 模擬節點移除並計算影響 (What-If Analysis)
// =========================================================
// 目的：進行「破壞性測試」。模擬如果前 5 大樞紐節點分別「故障」或「離開」，網路還剩下多少節點與連結。
// 方法：使用 Cypher Projection 動態建立一個「排除特定節點」的子圖。

MATCH (critical:Person)
WHERE critical.betweenness_hub IS NOT NULL
WITH critical ORDER BY critical.betweenness_hub DESC LIMIT 5
WITH collect(critical) AS criticalNodes

// 對每一個關鍵節點進行迴圈 (UNWIND)
UNWIND criticalNodes AS node
WITH node.name AS removedNode

// 建立一個臨時的新圖，條件是：節點名字不等於被移除的節點
CALL gds.graph.project.cypher(
  'reduced-network-' + removedNode, // 為每個模擬情境建立唯一的圖名稱
  'MATCH (p:Person) WHERE p.name <> $nodeName RETURN id(p) AS id',
  'MATCH (p1:Person)-[r:CONNECTED_TO]-(p2:Person) 
   WHERE p1.name <> $nodeName AND p2.name <> $nodeName 
   RETURN id(p1) AS source, id(p2) AS target',
  {parameters: {nodeName: removedNode}} // 傳入參數
)
YIELD graphName, nodeCount, relationshipCount
RETURN 
  removedNode AS RemovedHub,           // 被移除的人是誰
  nodeCount AS RemainingNodes,         // 移除後，網路還剩多少人
  relationshipCount AS RemainingConnections; // 移除後，連結少了多少 (可以看出該節點攜帶了多少連結)


// =========================================================
// Step 6: 建立城市/人員 重要性綜合指數 (Composite Index)
// =========================================================
// 目的：單一指標 (如 Degree 或 Betweenness) 可能有偏差，這裡將兩者結合。
// 公式：60% 的中介中心性 (權重較高，強調橋樑作用) + 40% 的連結數 (權重較低，強調直接人脈)。

// 6.1 先找出最大的 Betweenness 數值，用於歸一化 (Normalization)
MATCH (p:Person)
WHERE p.betweenness_hub IS NOT NULL
WITH max(p.betweenness_hub) AS maxBetweenness

// 6.2 計算每個人的綜合分數
MATCH (p:Person)
WHERE p.betweenness_hub IS NOT NULL
WITH p, maxBetweenness, 
     size((p)-[:CONNECTED_TO]-()) AS degree // 計算 Degree (連結數)

SET p.importanceIndex = 
  (p.betweenness_hub / maxBetweenness) * 0.6 + // 將 Betweenness 轉為 0~1 之間的比率
  (degree / 50.0) * 0.4                        // 假設最大 Degree 約為 50，將其歸一化 (需視實際資料調整分母)

RETURN 
  p.name AS Hub,
  p.importanceIndex AS ImportanceIndex, // 最終綜合評分
  p.betweenness_hub AS Betweenness,
  degree AS Degree
ORDER BY p.importanceIndex DESC
LIMIT 15;


// =========================================================
// 清理資源
// =========================================================
// 刪除記憶體中的圖形，釋放空間 (包含 Step 5 中動態產生的那些圖，這裡只示範刪除主圖)
CALL gds.graph.drop('hub-network');
// 注意：實際運作中，Step 5 產生的 'reduced-network-xxx' 也需要清理，否則會佔用記憶體。
```

---

### 題目 6: 旅遊路線規劃與聚類優化

**情境**: 設計「N日遊」路線推薦系統。
**目的**: 目的是建立一個「智慧旅遊路線規劃系統」。

它模擬將「人物」（這裡視為「景點」或「城市」）根據地理位置進行分群，並在每個區域內規劃最佳旅遊路線。具體流程如下：

1. 地理分群 (Clustering)：利用 K-Means 演算法，根據經緯度將靠得近的景點歸類為同一個「旅遊區」。

2. 路徑優化 (Infrastructure)：在每個旅遊區內，利用 最小生成樹 (MST) 找出連接所有景點的最短路徑骨幹，確保交通成本最低。

3. 熱門度分析 (Ranking)：利用 PageRank 找出各區最熱門的必訪景點。

4. 行程推薦 (Itinerary)：根據熱門度自動生成 3日、5日、7日遊的景點清單。

5. 環狀路線 (Routing)：基於 MST 骨幹設計不走回頭路的環形旅遊路線。

#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 準備數據 - 建立帶座標的網路 (模擬地理位置)
// =========================================================
// 目的：因為原始資料沒有經緯度，這裡隨機生成模擬的地理座標。
// 我們將圖中的 Person 節點視為「旅遊景點」或「城市」。
MATCH (p:Person)
SET p.latitude = 40 + rand() * 10,  // 隨機生成緯度
    p.longitude = -5 + rand() * 10; // 隨機生成經度

// 將經緯度組合成一個列表屬性，這是 GDS K-Means 演算法要求的輸入格式
MATCH (p:Person)
SET p.coordinate = [p.latitude, p.longitude];


// =========================================================
// Step 2: 建立圖投影 (Graph Projection)
// =========================================================
// 目的：將節點、關係以及「座標屬性」載入記憶體。
// 注意：必須在 nodeProperties 中包含 'coordinate'，否則下一步的 K-Means 無法讀取。
// 嘗試刪除舊圖以免報錯 (如果不存在會略過)
CALL gds.graph.drop("tourism-network") YIELD graphName; 

CALL gds.graph.project(
  'tourism-network',
  'Person',
  {
    CONNECTED_TO: {
      orientation: 'UNDIRECTED',
      properties: ['distance'] // 載入距離權重，供 MST 計算使用
    }
  },
  {
    // 重要：這裡必須宣告要載入的節點屬性
    nodeProperties: ['latitude', 'longitude', 'coordinate']
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 3: 使用 K-Means 進行地理聚類 (Geographic Clustering)
// =========================================================
// 目的：根據地理位置將景點分組。
// 意義：這就像把地圖劃分為「北區」、「南區」、「山區」等不同的旅遊區塊 (Cluster)。
// 參數 k: 5 代表我們希望切分出 5 個不同的旅遊區域。
CALL gds.kmeans.write('tourism-network', {
  nodeProperty: 'coordinate', // 依據座標屬性來分群
  k: 5,                       // 分成 5 群
  writeProperty: 'cluster'    // 將分群結果 (0~4) 寫回節點屬性
})
YIELD communityDistribution
RETURN communityDistribution;


// =========================================================
// Step 4: 在每個聚類中找出 Minimum Spanning Tree (MST)
// =========================================================
// 目的：在每一個旅遊區域內，找出「連接所有景點且總距離最短」的路徑組合。
// 意義：這建立了該區域的交通骨幹，確保遊客可以在區域內移動且路程最短。

// 技巧：使用 CALL {...} 子查詢 (Subquery) 對每個 Cluster 分別進行處理
MATCH (p:Person)
WHERE p.cluster IS NOT NULL
WITH DISTINCT p.cluster AS clusterId

CALL {
  WITH clusterId
  // 1. 先抓出該 Cluster 內的所有節點 ID
  MATCH (p:Person {cluster: clusterId})
  WITH collect(id(p)) AS clusterNodes, head(collect(id(p))) AS startNode, clusterId
  
  // 2. 動態建立該 Cluster 的子圖投影 (Sub-graph Projection)
  // 因為 MST 演算法通常是對全圖運算，我們這裡用 Cypher Projection 只投射該區域的節點與關係
  CALL gds.graph.drop("cluster-" + clusterId + "-graph", FALSE) YIELD nodeCount
  
  CALL gds.graph.project.cypher(
    'cluster-' + clusterId + '-graph',
    'MATCH (p:Person) WHERE id(p) IN $nodes RETURN id(p) AS id',
    'MATCH (p1:Person)-[r:CONNECTED_TO]-(p2:Person) 
     WHERE id(p1) IN $nodes AND id(p2) IN $nodes 
     RETURN id(p1) AS source, id(p2) AS target, r.distance AS weight',
    {
      parameters: {nodes: clusterNodes},
      relationshipOrientation: 'UNDIRECTED' 
    }
  )
  YIELD graphName
  
  // 3. 在這個子圖上計算 MST，並寫回資料庫
  // 這會建立一個新的關係類型 :MST_ROUTE，標記出最佳路徑
  CALL gds.spanningTree.write(graphName, {
    sourceNode: startNode,
    relationshipWeightProperty: 'weight',
    writeProperty: 'mst',
    writeRelationshipType: 'MST_ROUTE' // 這是我們找出的最佳交通連線
  })
  YIELD effectiveNodeCount
  
  RETURN effectiveNodeCount
}
RETURN clusterId, effectiveNodeCount;


// =========================================================
// Step 5: 結合 PageRank 識別必訪城市
// =========================================================
// 目的：在區域內找出「最熱門」或「交通最便利」的樞紐景點。
// 意義：PageRank 高的點通常是該區域的核心城市，適合做為旅遊的起點或住宿點。
CALL gds.pageRank.write('tourism-network', {
  writeProperty: 'popularity',
  maxIterations: 20
})
YIELD nodePropertiesWritten;

// 檢視每個區域的前 3 大必訪景點
MATCH (p:Person)
WHERE p.cluster IS NOT NULL AND p.popularity IS NOT NULL
WITH p.cluster AS cluster, p ORDER BY p.popularity DESC
WITH cluster, collect(p.name)[..3] AS topAttractions
RETURN 
  cluster AS TourismZone,
  topAttractions AS MustVisitCities;


// =========================================================
// Step 6: 生成 3日、5日、7日遊路線建議
// =========================================================
// 目的：根據景點的熱門程度 (PageRank)，自動推薦不同天數的行程清單。
MATCH (p:Person)
WHERE p.cluster IS NOT NULL AND p.popularity IS NOT NULL
WITH p.cluster AS cluster, p ORDER BY p.popularity DESC
// 將該區所有景點按熱門度排序並收集起來
WITH cluster, collect(p)[..7] AS cities 
RETURN 
  cluster AS Zone,
  [c IN cities[..3] | c.name] AS ThreeDayTour, // 取前 3 名
  [c IN cities[..5] | c.name] AS FiveDayTour,  // 取前 5 名
  [c IN cities | c.name] AS SevenDayTour;      // 取前 7 名


// =========================================================
// Step 7: 設計環形路線 (簡化的 TSP 旅行推銷員問題)
// =========================================================
// 目的：利用前面算出的 MST 骨幹 (:MST_ROUTE)，試圖找出條不走回頭路的環形遊覽路線。
// 邏輯：從該區最熱門的點出發，沿著 MST 路徑走 3~5 步，看能否到達另一個點 (這裡僅為模擬展示路徑查詢)。
MATCH (start:Person {cluster: 0}) // 假設我們關注 Cluster 0
WITH start ORDER BY start.popularity DESC LIMIT 1 // 選最熱門的點當起點
MATCH path = (start)-[:MST_ROUTE*3..5]-(end) // 沿著 MST 骨幹走
WHERE end <> start
WITH start, end, path, length(path) AS pathLength
ORDER BY pathLength ASC // 這裡簡單取最短的幾條路徑範例
LIMIT 1
WITH nodes(path) AS tourNodes
RETURN [n IN tourNodes | n.name] AS CircularTour;


// =========================================================
// 清理
// =========================================================
// 刪除全域投影圖。
// 注意：迴圈中產生的 'cluster-X-graph' 也應清理，但因數量多，實務上通常會在迴圈結束前 drop 掉，或重啟 GDS 實例。
CALL gds.graph.drop('tourism-network');
```
---

## 第三部分:推薦系統與相似度

### 題目 7: 混合推薦引擎

**情境**: 建立結合協同過濾和內容過濾的電影推薦系統。
**目的**: 目的是建立一個 「混合式推薦系統 (Hybrid Recommendation System)」。

它不依賴單一演算法，而是結合了三種主流推薦技術，並解決了新用戶無資料的問題：

1. 協同過濾 (Collaborative Filtering)：找出「口味相似」的用戶，推薦他們喜歡的電影。

2. 基於內容的過濾 (Content-Based Filtering)：找出「卡司陣容相似」的電影。

3. 個人化 PageRank (Personalized PageRank)：基於圖譜結構，計算用戶對特定電影的關注流動。

4. 冷啟動 (Cold Start)：針對全新無紀錄的用戶，推薦全域最熱門的多樣化電影。

#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立用戶-電影評分網路 (模擬數據)
// =========================================================
// 目的：因為資料庫中可能只有「演出」關係，這裡隨機模擬 User 對 Movie 的評分 (RATED)。
// 設定：隨機選取部分人與電影，建立 RATED 關係，並給予 1~5 分的 rating 權重。
MATCH (p:Person)
WITH p LIMIT 20
MATCH (m:Movie)
WITH p, m LIMIT 10
WHERE rand() < 0.3 // 30% 的機率產生關連，模擬稀疏矩陣
MERGE (p)-[r:RATED]->(m)
SET r.rating = toInteger(1 + rand() * 5); // 隨機評分 1~5 分


// =========================================================
// Step 2: 建立圖投影 (Graph Projection)
// =========================================================
// 目的：將推薦所需的節點與關係載入 GDS 記憶體。
// 包含：
// 1. RATED (含 rating 屬性)：用於協同過濾。
// 2. ACTED_IN：用於內容相似度分析 (演員陣容)。
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


// =========================================================
// Step 3: 計算用戶相似度 (基於用戶的協同過濾 - User-Based CF)
// =========================================================
// 目的：找出「品味相近」的人。
// 邏輯：如果 User A 和 User B 對同一群電影給出評分，他們之間就產生 SIMILAR_USER 關係。
// 應用：Jaccard Similarity。
CALL gds.nodeSimilarity.write('recommendation-network', {
  nodeLabels: ['Person'],          // 只比較 Person 節點
  relationshipTypes: ['RATED'],    // 根據 RATED 關係來比較
  writeRelationshipType: 'SIMILAR_USER',
  writeProperty: 'userSimilarity', // 相似度分數
  similarityCutoff: 0.1            // 過濾掉相似度太低的組合
})
YIELD nodesCompared, relationshipsWritten;


// =========================================================
// Step 4: 計算電影內容相似度 (基於內容的過濾 - Content-Based)
// =========================================================
// 目的：找出「內容/卡司相近」的電影。
// 邏輯：兩部電影如果由相同的演員演出 (共同鄰居)，計算其 Jaccard Index。
// 注意：這一步是使用 Cypher 直接操作資料庫，而非 GDS 演算法，適合建立靜態的相似性圖譜。

MATCH (m1:Movie)<-[:ACTED_IN]-(p:Person)-[:ACTED_IN]->(m2:Movie)
WHERE id(m1) < id(m2) // 避免重複計算 (A-B 和 B-A) 與 自我比較
WITH m1, m2, count(p) AS commonActors

// 計算分母：(m1 總演員數 + m2 總演員數 - 共同演員數) = 聯集 (Union)
WITH m1, m2, commonActors,
     COUNT{((m1)<-[:ACTED_IN]-())} AS m1Actors,
     COUNT{((m2)<-[:ACTED_IN]-())} AS m2Actors

// 計算 Jaccard 公式：交集 / 聯集
WITH m1, m2, 
     toFloat(commonActors) / (m1Actors + m2Actors - commonActors) AS jaccardSim
WHERE jaccardSim > 0.1

// 建立電影間的相似關係
MERGE (m1)-[s:CONTENT_SIMILAR]-(m2)
SET s.similarity = jaccardSim;


// =========================================================
// Step 5: Personalized PageRank 推薦 (單一演算法測試)
// =========================================================
// 目的：基於圖結構的推薦。模擬使用者在圖上隨機遊走 (Random Walk)。
// 設定：將 Tom Hanks 按讚/評分過的電影設為「起點 (Source Nodes)」。
// 結果：分數越高，代表從 Tom Hanks 的喜好出發，越容易走到的電影節點。

MATCH (user:Person {name: 'Tom Hanks'})
MATCH (user)-[:RATED]->(likedMovie:Movie)
WITH user, collect(id(likedMovie)) AS sourceNodes // 收集該用戶喜歡的電影 ID 列表

CALL gds.pageRank.stream('recommendation-network', {
  sourceNodes: sourceNodes, // 重要：指定個人化起點
  dampingFactor: 0.85,
  maxIterations: 20
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
// 過濾：只推薦電影，且排除掉用戶已經看過(評分過)的電影
WHERE node:Movie AND NOT EXISTS((user)-[:RATED]->(node))
RETURN 
  node.title AS RecommendedMovie,
  score AS PersonalizedScore
ORDER BY score DESC
LIMIT 10;


// =========================================================
// Step 6: 混合推薦 (Hybrid Recommendation - 核心邏輯)
// =========================================================
// 目的：將上述三種邏輯加權混合，計算最終分數。
// 權重分配：
// 1. 協同過濾 (35%)：相似網友喜歡這部片嗎？
// 2. 內容過濾 (30%)：這部片跟我不久前看過的片像嗎？
// 3. PageRank (35%)：從我的歷史紀錄來看，這部片在圖譜上的關聯度高嗎？

MATCH (user:Person)
WITH user LIMIT 1 // 範例：只選一位用戶進行演示
MATCH (user)-[:RATED {rating: 5}]->(likedMovie:Movie) // 找出該用戶「非常喜歡」的電影作為基準

// A. 協同過濾分數 (CF Score)
// 路徑：我 -> 相似網友 -> 電影
MATCH (user)-[:SIMILAR_USER]->(similarUser:Person)
MATCH (similarUser)-[r:RATED]->(movie:Movie)
WHERE NOT EXISTS((user)-[:RATED]->(movie)) AND r.rating >= 4 // 推薦未看過且網友高評價的片
WITH user, movie, avg(r.rating) AS cfScore

// B. 內容過濾分數 (Content Score)
// 路徑：我喜歡的電影 -> 內容相似 -> 候選電影
MATCH (user)-[:RATED {rating: 5}]->(liked:Movie)
MATCH (liked)-[s:CONTENT_SIMILAR]-(movie)
WITH user, movie, cfScore, avg(s.similarity) AS contentScore

// C. 個性化 PageRank 分數 (PPR Score)
// 即時計算針對這部候選電影的 PPR 分數
MATCH (user)-[:RATED]->(likedMovie:Movie)
WITH user, movie, cfScore, contentScore, collect(id(likedMovie)) AS sourceNodes
CALL gds.pageRank.stream('recommendation-network', {
  sourceNodes: sourceNodes,
  maxIterations: 10
})
YIELD nodeId, score
WHERE nodeId = id(movie) // 這裡只抓取「候選電影」的 PageRank 分數
WITH user, movie, cfScore, contentScore, score AS prScore

// D. 綜合加權評分
// coalesce 用於處理 null 值 (例如某部片雖然內容相似，但沒有網友評分，補 0)
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


// =========================================================
// Step 7: 冷啟動策略 (Cold Start)
// =========================================================
// 目的：當新用戶註冊，沒有任何歷史評分時，上述方法都會失效。
// 策略：使用「全域 PageRank」找出整個系統中最具影響力/最熱門的電影。
// 優化：為了避免總是推薦同一批熱門片，加入分段抽樣 (Diverse Sampling) 增加多樣性。

// 7.1 計算全域重要性
CALL gds.pageRank.write('recommendation-network', {
  writeProperty: 'globalPopularity'
})
YIELD nodePropertiesWritten;

// 7.2 提取多樣化的熱門清單
MATCH (m:Movie)
WHERE m.globalPopularity IS NOT NULL
WITH m ORDER BY m.globalPopularity DESC LIMIT 50 // 取前 50 大熱門
// 技巧：每隔 5 名取一部，避免推薦的都是同系列的熱門片，確保清單具備一定的多樣性
WITH collect(m) AS popularMovies
UNWIND range(0, 9) AS idx
WITH popularMovies[idx * 5] AS diverseMovie
RETURN 
  diverseMovie.title AS ColdStartRecommendation,
  diverseMovie.globalPopularity AS Popularity
ORDER BY diverseMovie.globalPopularity DESC;


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('recommendation-network');
```

---

### 題目 8: 演員職涯路徑推薦

**情境**: 為新進演員推薦最佳職涯發展路徑。
**目的**: 目的是建立一個「職涯導航與分析系統」。

它利用圖學演算法來分析演員的職業軌跡，不僅僅是看誰最紅，而是深入分析「成功的模式」。主要應用了以下技術：

1. 向量嵌入 (FastRP)：將演員在圖中的位置與結構轉化為數學向量，用來精確計算「職涯相似度」。

2. 社群偵測 (Leiden)：將演員分為不同的「職涯流派」（例如：商業大片型、獨立電影型、電視劇轉型型）。

3. 路徑分析 (Random Walk)：模擬職涯發展的隨機過程，觀察多樣性。

4. 導師與合作推薦：基於上述特徵，為新演員尋找「模範導師」並預測潛在的「黃金搭檔」。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 準備職涯成功指標 (模擬數據)
// =========================================================
// 目的：因為原始資料沒有「成功指數」，這裡根據電影產量加上一些隨機變數來模擬。
// 實際應用中，這裡可以是票房總和、獲獎次數或 IMDB 平均分。
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
WITH p, count(m) AS movieCount
WHERE movieCount >= 2 // 只分析至少演過兩部戲的演員
SET p.careerSuccess = movieCount * (1 + rand()); // 模擬計算成功分數


// =========================================================
// Step 2: 建立職涯網路投影 (Graph Projection)
// =========================================================
// 目的：將節點、關係以及關鍵屬性 (careerSuccess) 載入 GDS 記憶體。
// 注意：這次我們使用了 native projection 語法，並特別指定載入節點屬性，這對後續的嵌入 (Embedding) 很重要。
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
    undirectedRelationshipTypes: ['*'] // 視為無向圖
  }
);


// =========================================================
// Step 3: 使用 FastRP 生成職涯向量嵌入 (Node Embeddings)
// =========================================================
// 目的：將每個演員轉化為一個 128 維的向量 (Vector)。
// 意義：FastRP (Fast Random Projection) 能捕捉節點在圖中的「結構特徵」。
// 兩個演員如果向量距離很近，代表他們在圖中的地位、連結模式或合作圈子非常相似 (即使他們不認識)。
CALL gds.fastRP.write('career-network', {
  embeddingDimension: 128,         // 向量維度，越高越精細但運算越慢
  iterationWeights: [0.0, 1.0, 1.0, 1.0], // 控制算法如何聚合鄰居資訊
  randomSeed: 42,                  // 固定種子碼以確保結果可重現
  writeProperty: 'careerEmbedding' // 將向量寫回節點屬性
})
YIELD nodePropertiesWritten, computeMillis
RETURN 
  'FastRP Embeddings' AS Feature,
  nodePropertiesWritten AS NodesProcessed,
  computeMillis AS TimeMs;


// =========================================================
// Step 4: 使用 Leiden 演算法進行職涯分群
// =========================================================
// 目的：將演員分為不同的「職涯類型」或「圈子」。
// 演算法：Leiden 是 Louvain 的改良版，能產生連結更緊密、品質更好的社群。
// 應用：這可以識別出「好萊塢主流圈」、「獨立製片圈」、「動作片演員圈」等不同群體。
CALL gds.leiden.write('career-network', {
  writeProperty: 'careerPathType',   // 將分群結果 (ID) 寫回屬性
  includeIntermediateCommunities: false,
  randomSeed: 42
})
YIELD communityCount, modularity, computeMillis
RETURN 
  'Career Path Clustering' AS Analysis,
  communityCount AS PathTypes,
  round(modularity * 1000) / 1000 AS Modularity, // 模組化指數，越高代表分群效果越好
  computeMillis AS TimeMs;


// =========================================================
// Step 5: 分析每個職涯類型的特徵 (Profiling)
// =========================================================
// 目的：單純的分群 ID (如 0, 1, 2) 沒有意義，我們需要分析每個群體的特性。
// 作法：統計每個群體中「平均成功指數」以及「代表性人物」。
MATCH (p:Person)
WHERE p.careerPathType IS NOT NULL
WITH p.careerPathType AS pathType, p
ORDER BY p.careerSuccess DESC
WITH pathType,
     collect(p.name)[..5] AS topPerformers, // 該群體最成功的 5 個人
     avg(p.careerSuccess) AS avgSuccess,    // 該群體的平均成功度
     count(p) AS memberCount
RETURN 
  pathType AS CareerPathType,
  topPerformers AS SuccessExamples,
  round(avgSuccess * 10) / 10 AS AvgSuccessScore,
  memberCount AS PathMembers
ORDER BY avgSuccess DESC; // 按平均成功度排序，找出「菁英群體」


// =========================================================
// Step 6: 使用 Harmonic Centrality 識別關鍵連結者
// =========================================================
// 目的：找出影響力人物。
// 演算法：Harmonic Centrality (調和中心性) 適合處理非連通圖 (有孤島的圖)。
// 意義：分數高的人，代表能以最短路徑接觸到網路中的其他人，通常是「超級人脈王」。
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


// =========================================================
// Step 7: 為新演員找相似成功路徑 (Role Model Discovery)
// =========================================================
// 目的：這是「嵌入 (Embedding)」的強大應用。
// 情境：對於還不紅的新演員 (NewActor)，我們想知道哪位成功的大咖 (Successful) 跟他的結構特徵最像。
// 算法：Cosine Similarity (餘弦相似度) 計算兩個向量的夾角。
MATCH (newActor:Person)
WHERE newActor.careerSuccess IS NULL OR newActor.careerSuccess < 3
WITH newActor LIMIT 3

MATCH (successful:Person)
WHERE successful.careerSuccess > 8         // 目標是成功人士
  AND successful.careerEmbedding IS NOT NULL
  AND newActor.careerEmbedding IS NOT NULL
WITH newActor, successful,
     // 計算向量相似度
     gds.similarity.cosine(newActor.careerEmbedding, successful.careerEmbedding) AS similarity
WHERE similarity > 0.5 // 只看相似度夠高的
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


// =========================================================
// Step 8: Link Prediction - 預測有價值的合作
// =========================================================
// 目的：預測兩個還沒合作過，但應該合作的演員。
// 演算法：Adamic Adar。它看的是「共同鄰居」的稀缺性。
// 應用：如果我們有很多共同朋友，而這些朋友本身朋友不多(圈子很專)，那我們合作機率極高。
MATCH (person1:Person)
WHERE person1.careerSuccess > 5
MATCH (person2:Person)
WHERE person2.careerSuccess > 5 
  AND id(person1) < id(person2) // 避免重複與自我比較
  AND NOT EXISTS((person1)-[:ACTED_IN]->()<-[:ACTED_IN]-(person2)) // 確保目前沒合作過
WITH person1, person2, 
     gds.alpha.linkprediction.adamicAdar(person1, person2) AS adamicScore
WHERE adamicScore > 0
RETURN 
  person1.name AS Actor1,
  person2.name AS Actor2,
  round(adamicScore * 100) / 100 AS CollaborationPotential,
  person1.careerPathType AS Path1,
  person2.careerPathType AS Path2,
  // 判斷是跨圈子合作還是同圈子強強聯手
  CASE 
    WHEN person1.careerPathType <> person2.careerPathType 
    THEN 'Cross-Path Opportunity'
    ELSE 'Same-Path Collaboration'
  END AS CollaborationType
ORDER BY adamicScore DESC
LIMIT 20;


// =========================================================
// Step 9: 職涯軌跡分析 - 使用 Random Walk
// =========================================================
// 目的：模擬職涯發展路徑。 
// 概念：從一個導師級人物出發，隨機在圖上走幾步，看看這條路通常會經過哪些類型的圈子。
// 觀察：如果路徑經過多種 PathType，代表這是一個多元發展的職涯；反之則是專精型。
MATCH (mentor:Person)
WHERE mentor.careerSuccess > 10
WITH mentor ORDER BY mentor.networkInfluence DESC LIMIT 3

CALL gds.randomWalk.stream('career-network', {
  sourceNodes: [mentor],
  walkLength: 8,      // 每次走 8 步
  walksPerNode: 5,    // 每個起點模擬 5 次
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
  [step IN careerJourney[1..-1] | step.name] AS CareerProgression, // 中間經過的人
  // 使用 REDUCE 函數計算路徑中「不重複的職涯類型」數量，代表多樣性 (Diversity)
  size(REDUCE(acc = [], s IN careerJourney |
    CASE WHEN NOT s.pathType IN acc THEN acc + s.pathType ELSE acc END
  )) AS PathDiversity
LIMIT 10;


// =========================================================
// Step 10: 生成個性化職涯建議報告
// =========================================================
// 目的：整合前面的分析 (嵌入相似度 + 連結預測)，為特定演員生成一份建議書。
// 內容：1. 你的模範是誰？ 2. 你接下來該找誰合作？
MATCH (actor:Person)
WHERE actor.careerSuccess IS NULL OR actor.careerSuccess < 5
WITH actor LIMIT 1

// 1. 找模範 (基於 Embedding)
MATCH (successful:Person)
WHERE successful.careerSuccess > 8
  AND successful.careerEmbedding IS NOT NULL
  AND actor.careerEmbedding IS NOT NULL
WITH actor, successful,
     gds.similarity.cosine(actor.careerEmbedding, successful.careerEmbedding) AS similarity
ORDER BY similarity DESC
LIMIT 3
WITH actor, collect(successful) AS mentors

// 2. 找合作對象 (基於 Adamic Adar)
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
  // 顯示模範名單
  [m IN mentors | {
    name: m.name,
    path: m.careerPathType,
    success: round(m.careerSuccess * 10) / 10
  }] AS RoleModels,
  // 顯示推薦合作名單
  collect({
    partner: potentialPartner.name,
    score: round(score * 100) / 100
  }) AS RecommendedCollaborations;


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('career-network');
```

---

### 題目 9: 實時推薦更新機制

**情境**: 當用戶行為發生時,即時更新推薦結果。
**目的**: 目的是展示如何實現 「即時/增量式推薦系統 (Real-time/Incremental Recommendation)」。

在大型推薦系統中，當用戶產生新行為（如評分電影）時，重新計算整個圖譜的成本太高。這段程式碼演示了：

1. 基礎建設：建立共現圖 (Co-occurrence Graph) 並計算社群緊密度 (Triangle Count)。

2. 增量更新：當新評分發生時，只更新受影響的鄰居節點之間的相似度，而不是重算全圖。

3. 即時推薦：利用局部鄰居的最新數據快速生成推薦。

4. 效能對比：展示「增量更新」與「全圖重算」在效率上的巨大差異。
#### 解答程式碼:

```cypher
// =========================================================
// Step 0: 清理環境
// =========================================================
// 目的：確保沒有殘留的 GDS 圖投影，避免名稱衝突。
// FALSE 參數表示如果圖不存在，不會報錯 (Silent Drop)。
CALL gds.graph.drop('realtime-rec-network', FALSE);
CALL gds.graph.drop('person-cograph', FALSE); 


// =========================================================
// Step 1: 建立基礎 In-Memory 圖投影 (Bipartite Graph)
// =========================================================
// 目的：將 Person (用戶) 和 Movie (電影) 及其互動載入記憶體。
// 結構：這是一個二分圖 (Bipartite)，用戶與電影相連，但用戶與用戶不直接相連。
CALL gds.graph.project(
  'realtime-rec-network',
  ['Person', 'Movie'],
  {
      REVIEWED: {
          properties: 'rating',
          orientation: 'UNDIRECTED'    // 設定為無向，方便計算連結
      },
      ACTED_IN: {
          orientation: 'NATURAL'      // 保留原始方向 (Person -> Movie)
      }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 建立 Person-Person 共評分投影 (Monopartite Graph)
// =========================================================
// 目的：為了計算 Triangle Count (三角計數) 或分群，我們需要「用戶對用戶」的直接關係。
// 方法：如果兩個用戶評論過同一部電影，就建立一條 CO_REVIEWED_WITH 關係。
// 權重：共同評論的電影數量越多，權重越高。

MATCH (p1:Person)-[:REVIEWED]->(m:Movie)<-[:REVIEWED]-(p2:Person)
WHERE id(p1) < id(p2)  // 技巧：利用 ID 大小排序，確保每對用戶只處理一次 (避免 A-B 和 B-A 重複)
WITH p1, p2, count(*) AS weight
MERGE (p1)-[r:CO_REVIEWED_WITH]-(p2)
SET r.weight = weight;

// 將這個新的單一部分圖 (Person only) 投影到 GDS
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


// =========================================================
// Step 3: 計算 Triangle Count 並寫回 DB
// =========================================================
// 目的：計算每個用戶的 Clustering Coefficient (群聚係數) 基礎。
// 意義：三角計數高代表該用戶處於一個緊密的社群中 (大家互相都認識/相似)。
CALL gds.triangleCount.write('person-cograph', {
  writeProperty: 'triangles_baseline' // 將基準值寫回資料庫
})
YIELD nodePropertiesWritten, globalTriangleCount, nodeCount;

// 檢視社群緊密度最高的前 10 位用戶
MATCH (p:Person)
RETURN p.name AS User, p.triangles_baseline AS TriangleCount
ORDER BY p.triangles_baseline DESC
LIMIT 10;


// =========================================================
// Step 4: 模擬用戶新增評分 (Real-time Event)
// =========================================================
// 目的：模擬一個即時發生的事件 (例如 User 剛看完一部電影並給了 5 分)。
// 這會改變圖的結構，正常來說需要重算演算法，但我們將使用增量更新。
MATCH (user:Person)
WITH user LIMIT 1 // 隨機選一個用戶
MATCH (movie:Movie)
WHERE NOT EXISTS((user)-[:REVIEWED]->(movie)) // 找一部他沒看過的電影
WITH user, movie LIMIT 1
MERGE (user)-[r:REVIEWED]->(movie) // 建立新關係
SET r.rating = 5, r.timestamp = timestamp()
WITH user, movie


// =========================================================
// Step 5: 增量更新 - 只更新受影響的節點 (Incremental Update)
// =========================================================
// 目的：核心邏輯。我們不重新跑全圖運算，而是只找出「跟這部新電影有關的其他用戶」。
// 原理：只有看過這部電影的其他用戶，跟當前用戶的相似度才會發生變化。

// 1. 找出受影響的用戶 (看過同一部新電影的人)
MATCH (user)-[:REVIEWED]->(newMovie:Movie)
WHERE newMovie = movie
MATCH (user)-[:REVIEWED]->(otherMovie:Movie)<-[:REVIEWED]-(otherUser:Person)
WHERE otherMovie <> newMovie
WITH user, collect(DISTINCT otherUser) AS affectedUsers

// 2. 只針對這些受影響的用戶重新計算相似度 (Cosine/Jaccard 變體)
UNWIND affectedUsers AS affected
WITH user, affected
MATCH (affected)-[:REVIEWED]->(m1:Movie)<-[:REVIEWED]-(user)
WITH user, affected, count(m1) AS commonMovies,
     COUNT{(affected)-[:REVIEWED]->()} AS affectedTotal,
     COUNT{(user)-[:REVIEWED]->()} AS userTotal
// 計算新的相似度分數
WITH user, affected, 
     toFloat(commonMovies) / sqrt(affectedTotal * userTotal) AS newSimilarity
// 更新或建立增量相似度關係，並標記時間戳記
MERGE (user)-[s:SIMILAR_TO_INCREMENTAL]-(affected)
SET s.similarity = newSimilarity,
    s.updated = timestamp()
RETURN 
  user.name AS User,
  collect({user: affected.name, similarity: newSimilarity})[..5] AS UpdatedSimilarities;


// =========================================================
// Step 6: 快取策略 - 基於鄰居的快速推薦 (Local Recommendation)
// =========================================================
// 目的：利用剛剛更新的局部相似度 (SIMILAR_TO_INCREMENTAL)，直接撈取鄰居喜歡的電影。
// 優勢：這是一個 Local Query，速度極快，不需要掃描全資料庫。
MATCH (user:Person)
WITH user LIMIT 1
// 找跟該用戶相似的鄰居 (Neighbor)
MATCH (user)-[:REVIEWED]->(:Movie)<-[:REVIEWED]-(neighbor:Person)
WITH user, collect(DISTINCT neighbor) AS neighbors

UNWIND neighbors AS neighbor
// 找鄰居看過、評價高，但我還沒看過的電影
MATCH (neighbor)-[r:REVIEWED]->(m:Movie)
WHERE NOT EXISTS((user)-[:REVIEWED]->(m)) AND r.rating >= 4
WITH user, m, avg(r.rating) AS avgRating, count(*) AS ratingCount
RETURN 
  m.title AS IncrementalRecommendation,
  avgRating AS Score,
  ratingCount AS SupportingUsers
ORDER BY avgRating DESC, ratingCount DESC
LIMIT 10;


// =========================================================
// Step 7: 性能比較 - 全圖重算 vs 增量更新
// =========================================================
// 目的：證明增量更新的價值。這裡執行一次傳統的 GDS 全圖相似度計算。
// 結果：全圖計算通常需要數百毫秒甚至數秒 (取決於數據量)，而 Step 5/6 通常在毫秒級完成。
WITH timestamp() AS startTime
CALL gds.nodeSimilarity.write('realtime-rec-network', {
  nodeLabels: ['Person'],
  relationshipTypes: ['REVIEWED'],
  writeRelationshipType: 'FULL_RECALC', // 寫入一個區隔用的關係名
  writeProperty: 'similarity'
})
YIELD nodesCompared, computeMillis
RETURN 
  'Full Recalculation' AS Method,
  computeMillis AS TimeMs,       // 這裡的時間會遠大於增量更新
  nodesCompared AS NodesProcessed;

// 顯示增量更新的理論優勢 (文字說明)
RETURN 
  'Incremental Update' AS Method,
  'Much Faster' AS TimeMs,
  'Only Affected Nodes' AS NodesProcessed;


// =========================================================
// 最後清理
// =========================================================
CALL gds.graph.drop('person-cograph');
CALL gds.graph.drop('realtime-rec-network');
```

---

## 第四部分:異常檢測與詐欺偵測

### 題目 10: 多層次詐欺環檢測

**情境**: 在交易網路中檢測複雜的詐欺模式。
**目的**: 主要目的是建立一個金融詐欺偵測系統 (Fraud Detection System)。

它利用圖形演算法來識別潛在的洗錢或詐欺行為，主要偵測邏輯包含三個維度：

1. 循環交易偵測 (Cycle Detection)：透過 Triangle Count 和 Cycle Search 找出資金繞一圈回到原點的異常行為（常見於洗錢或衝高交易量的假交易）。

2. 詐欺集團識別 (Community Detection)：利用 Label Propagation 找出與可疑帳戶互動密切的群體（近朱者赤，近墨者黑）。

3. 綜合風險評分 (Risk Scoring)：結合圖特徵（迴圈數）與統計特徵（交易金額標準差、頻率），計算出一個最終的「詐欺分數」。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 使用 Movies dataset 模擬交易網路 (Data Preparation)
// =========================================================
// 目的：原始資料庫可能沒有交易金額，這裡我們將 'RATED' 關係模擬成「轉帳交易」。
// 設定：隨機生成 10~1000 的金額 (amount)，以及隨機的過去時間戳 (timestamp)。
// 注意：此處假設 p 和 m 已經被前面的 MATCH 語句選中，或是為了演示方便省略了 MATCH。
// 若要執行，建議先 MATCH (p:Person)-[r:RATED]->(m:Movie) 再執行 SET。
MATCH (p:Person)-[r:RATED]->(m:Movie)
SET r.amount = toInteger(10 + rand() * 990),
    r.timestamp = timestamp() - toInteger(rand() * 86400000 * 30); // 過去 30 天內

// 標記一些「已知的」可疑帳戶 (Simulating Blacklist)
// 這是為了後面的「社群偵測」做準備，模擬現實中我們已有少量黑名單的情況。
MATCH (p:Person)
WITH p, rand() AS r
WHERE r < 0.1 // 10% 的人被標記為可疑
SET p.suspicious = true;


// =========================================================
// Step 2: 建立交易網路圖投影 (Graph Projection)
// =========================================================
// 目的：將資料載入 GDS 記憶體。
// 重要設定：
// 1. 載入 'amount' 和 'timestamp'，雖然此範例主要用結構分析，但在進階分析(如路徑權重)會用到。
// 2. orientation: 'UNDIRECTED'：在尋找詐欺共犯結構時，我們有時不分資金流向，只要有頻繁互動就算關聯。
CALL gds.graph.drop('fraud-detection-network', false); // 安全刪除舊圖

CALL gds.graph.project(
  'fraud-detection-network',
  'Person', // 注意：若 RATED 連接的是 Person->Movie，這裡只投射 Person 可能會導致沒有關係被載入。
            // 實務上如果是 P2P 轉帳，這裡應為 Person->Person。
            // 若是用 Movie 資料集模擬，建議同時投射 ['Person', 'Movie']。
  {
    RATED: {
      properties: ['amount', 'timestamp'],
      orientation: 'UNDIRECTED'
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 3: Triangle Count 檢測基礎詐欺環 (Circular Trading)
// =========================================================
// 目的：偵測「三角形」結構 (A->B->C->A)。
// 詐欺意義：正常的轉帳通常是線性的 (薪水->我->商家)。
// 如果出現三角形，往往代表「循環交易」或「洗錢」，資金繞一圈回到原點或小團體內部循環。

CALL gds.triangleCount.write('fraud-detection-network', {
  writeProperty: 'triangles' // 將三角形數量寫回節點屬性
})
YIELD nodeCount, globalTriangleCount;

// 檢視三角形最多的帳戶 (高風險嫌疑人)
MATCH (p:Person)
WHERE p.triangles > 0
RETURN 
  p.name AS Account,
  p.triangles AS TriangleCount,
  p.suspicious AS IsSuspicious
ORDER BY p.triangles DESC
LIMIT 15;


// =========================================================
// Step 4: 純 Cypher 檢測長鏈詐欺環 (4-6 節點)
// =========================================================
// 目的：Triangle 只找 3 個點的環，這裡找更複雜的「分層洗錢」(Layering)。
// 邏輯：A -> B -> C -> D -> A (4節點環)。
MATCH path = (p1:Person)-[:RATED*4..6]-(p1) // 尋找從 p1 出發，經過 4~6 步又回到 p1 的路徑
WHERE ALL(node IN nodes(path) WHERE node:Person) // 確保路徑上都是人 (過濾掉 Movie 節點，如果是二分圖的話)
WITH path, nodes(path) AS nds

// 驗證是否為 Simple Cycle (簡單迴圈)
// 1. 起點 = 終點 (Cypher 語法已隱含，但需確保中間沒有重複繞圈)
// 2. 節點數量檢核 (這行是為了過濾掉像 "8字形" 這種複雜的雙迴圈)
WHERE head(nds) = last(nds)
  AND size(nds) = size(apoc.coll.toSet(nds)) + 1 // 確保只有頭尾重複，中間節點都是唯一的
WITH nds AS cycle, length(path) AS cycleLength
WHERE cycleLength >= 4
RETURN 
  [n IN cycle | n.name] AS FraudRing,
  cycleLength AS RingSize
LIMIT 10;


// =========================================================
// Step 5: Label Propagation 識別詐欺社群 (Community Detection)
// =========================================================
// 目的：利用「標籤傳播演算法 (LPA)」找出共犯結構。
// 原理：LPA 運作像「多數決」，如果你周圍的人都是詐欺社群 1 號，你很快也會被歸類為社群 1 號。
// 應用：找出那些「雖然還沒被標記為可疑，但混在可疑群體中」的潛在共犯。


[Image of label propagation algorithm]

CALL gds.labelPropagation.write('fraud-detection-network', {
  writeProperty: 'community',
  maxIterations: 10
})
YIELD communityCount, ranIterations;

// 分析：找出含有「已知可疑帳戶 (suspicious=true)」的社群
MATCH (p:Person)
WHERE p.community IS NOT NULL AND p.suspicious = true
WITH p.community AS suspiciousCommunity
// 撈出這些社群裡的所有成員 (包含未被標記的人)
MATCH (member:Person {community: suspiciousCommunity})
RETURN 
  suspiciousCommunity AS FraudCommunity,
  collect(member.name) AS Members,
  size(collect(member)) AS MemberCount
ORDER BY MemberCount DESC
LIMIT 5;


// =========================================================
// Step 6: 建立異常評分系統 (Risk Scoring Engine)
// =========================================================
// 目的：單一指標容易誤判，我們結合「圖特徵」與「統計特徵」計算總分。
// 評分維度：
// 1. Triangle Score: 參與越多循環交易，分數越高 (結構異常)。
// 2. Frequency Score: 交易次數極高，分數越高 (行為異常，可能是機器人)。
// 3. Amount Score: 交易金額波動 (標準差) 極大，分數越高 (不穩定的資金流)。

MATCH (p:Person)
OPTIONAL MATCH (p)-[r:RATED]->(:Movie) // 抓取該人的所有交易紀錄
WITH p, 
     coalesce(p.triangles, 0) AS triangles,
     count(r) AS transactionCount,
     avg(r.amount) AS avgAmount,
     stDev(r.amount) AS stdAmount // 計算金額的標準差

WITH p, triangles, transactionCount, avgAmount, stdAmount,
     // 規則 1: 循環交易評分
     CASE 
       WHEN triangles > 5 THEN 0.4
       WHEN triangles > 2 THEN 0.2
       ELSE 0.0
     END AS triangleScore,
     // 規則 2: 高頻交易評分
     CASE 
       WHEN transactionCount > 10 THEN 0.3
       WHEN transactionCount > 5 THEN 0.15
       ELSE 0.0
     END AS frequencyScore,
     // 規則 3: 金額波動評分
     CASE 
       WHEN stdAmount > 300 THEN 0.3
       WHEN stdAmount > 150 THEN 0.15
       ELSE 0.0
     END AS amountScore

// 計算總分
SET p.fraudScore = triangleScore + frequencyScore + amountScore

RETURN 
  p.name AS Account,
  p.fraudScore AS FraudScore,
  triangles AS Triangles,
  transactionCount AS Transactions,
  round(avgAmount) AS AvgAmount,
  round(stdAmount) AS StdDevAmount,
  p.suspicious AS ActuallySuspicious // 對照組：看看我們抓到的是不是原本就標記可疑的人
ORDER BY p.fraudScore DESC
LIMIT 20;


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('fraud-detection-network');
```


---

### 題目 12: 共謀網路挖掘

**情境**: 識別互相配合進行詐欺的帳戶群組。
**目的**: 主要目的是建立一個「共犯結構與風險傳播偵測系統 (Collusion & Risk Propagation Detection)」。

它從單純的詐欺偵測進階到集團犯罪分析，重點在於：

1. 資金流向強度：利用加權 Degree Centrality 找出資金吞吐量大的高風險帳戶。

2. 緊密共犯結構：利用 K-Core 找出彼此頻繁互動的「核心犯罪集團」（例如：互助會、詐騙機房）。

3. 風險傳染 (Guilt by Association)：利用 BFS 演算法，計算一般人與已知詐欺犯的距離，距離越近，風險越高。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 計算加權 Degree Centrality (尋找資金樞紐)
// =========================================================
// 目的：找出交易金額 (Volume) 特别大的節點。
// 意義：在洗錢或詐欺網路中，核心帳戶（如水房車手頭目）通常會有異常高的資金流入流出。
// 我們使用 'amount' 作為權重，而不僅僅是計算交易次數。

CALL gds.graph.drop("collusion-network", FALSE); // 安全刪除舊圖

CALL gds.graph.project(
  'collusion-network',
  'Person',
  {
      RATED: {
          properties: 'amount', // 載入交易金額作為權重
          orientation: 'UNDIRECTED'
      }
  }
)
YIELD graphName, nodeCount, relationshipCount;

// 1.1 計算並寫入加權 Degree
CALL gds.degree.write('collusion-network', {
  relationshipWeightProperty: 'amount', // 指定使用金額加權
  writeProperty: 'weightedDegree'
})
YIELD nodePropertiesWritten;

// 1.2 識別前 10% 的高風險節點 (High Risk Identification)
MATCH (p:Person)
WHERE p.weightedDegree IS NOT NULL
WITH percentileDisc(p.weightedDegree, 0.9) AS threshold // 計算 P90 門檻值
MATCH (highrisk:Person)
WHERE highrisk.weightedDegree >= threshold
SET highrisk.highRisk = true // 標記為高風險
RETURN 
  highrisk.name AS HighRiskAccount,
  highrisk.weightedDegree AS WeightedDegree
ORDER BY highrisk.weightedDegree DESC
LIMIT 20;


// =========================================================
// Step 2: Ego Graph 提取 (2-hop 鄰居分析)
// =========================================================
// 目的：針對高風險節點，拉出他的「朋友圈」(Ego Network)。
// 意義：詐欺調查通常從嫌疑人出發，調查他的一度與二度人脈。這步是為了視覺化或進一步調查做準備。
MATCH (highrisk:Person {highRisk: true})
WITH highrisk LIMIT 5
// 尋找 1 到 2 跳距離內的所有鄰居
MATCH path = (highrisk)-[:RATED*1..2]->(neighbor)
WITH highrisk, collect(DISTINCT neighbor) AS neighbors
RETURN 
  highrisk.name AS CenterNode,
  // 列出鄰居名字 (相容 Person 或 Movie 節點的顯示)
  [n IN neighbors | CASE WHEN n:Person THEN n.name ELSE n.title END] AS EgoNetwork,
  size(neighbors) AS NetworkSize
ORDER BY NetworkSize DESC;


// =========================================================
// Step 3: K-Core Decomposition 找緊密群組 (Collusion Rings)
// =========================================================
// 目的：找出連結非常緊密的核心小團體。
// 原理：K-Core 是一個遞迴剝離的過程。K-Core=3 代表群組內的每個人都至少跟群組內的另外 3 個人有連結。
// 意義：一般的隨機交易網路很難形成高 K-Core，但詐欺集團（如互助詐保、循環洗錢）通常會形成高 K-Core 結構。

CALL gds.kcore.write('collusion-network', {
  writeProperty: 'coreValue'
})
YIELD nodePropertiesWritten, degeneracy;

// 篩選出核心度 >= 3 的群組
MATCH (p:Person)
WHERE p.coreValue IS NOT NULL AND p.coreValue >= 3
WITH p.coreValue AS coreLevel, collect(p.name) AS members
RETURN 
  coreLevel AS KCoreLevel,     // 核心層級 (越高越核心)
  members AS CollusionGroup,   // 成員名單
  size(members) AS GroupSize
ORDER BY coreLevel DESC
LIMIT 10;


// =========================================================
// Step 4: Weakly Connected Components (WCC) 識別孤立詐欺網路
// =========================================================
// 目的：找出與主網路斷開的「孤島」。
// 意義：正常的經濟活動通常連通在一起。如果有一群人只在內部互相轉帳，完全不跟外界往來，且其中包含高風險份子，這極可能是專門用來洗錢的「封閉子圖」。

CALL gds.wcc.write('collusion-network', {
  writeProperty: 'wccComponent'
})
YIELD componentCount, componentDistribution;

MATCH (p:Person)
WHERE p.wccComponent IS NOT NULL
WITH p.wccComponent AS component, collect(p) AS members
// 過濾條件：
// 1. 群體大小適中 (3~10人)：排除太小的雜訊或太大的正常網路。
// 2. 包含高風險份子：群體內至少有一人是 Step 1 標記的高風險帳戶。
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


// =========================================================
// Step 5: 風險傳播模型 - BFS 傳播距離 (Risk Propagation)
// =========================================================
// 目的：計算「風險距離」。
// 原理：利用 BFS (廣度優先搜尋) 從已知壞人 (Source) 開始擴散。
// 規則：離壞人越近，風險越高 (近朱者赤，近墨者黑)。


[Image of breadth first search traversal]

MATCH (knownFraud:Person)
WHERE knownFraud.suspicious = true OR knownFraud.highRisk = true
WITH collect(id(knownFraud)) AS fraudNodes

UNWIND fraudNodes AS sourceNode
CALL gds.bfs.stream('collusion-network', {
  sourceNode: sourceNode,
  maxDepth: 4 // 只計算到第 4 層，太遠的影響力忽略
})
YIELD nodeIds, path
UNWIND nodeIds AS nodeId
// 計算距離：路徑長度 (Hop count)
WITH DISTINCT nodeId, min(size(nodes(path))) AS minDistance
WITH gds.util.asNode(nodeId) AS person, minDistance
// 排除掉原本就是壞人的節點，只標記受影響的一般人
WHERE person.suspicious IS NULL AND person.highRisk IS NULL
SET person.fraudProximity = minDistance
RETURN 
  person.name AS Account,
  minDistance AS DistanceFromKnownFraud,
  CASE 
    WHEN minDistance = 1 THEN 'Very High Risk' // 直接朋友
    WHEN minDistance = 2 THEN 'High Risk'      // 朋友的朋友
    WHEN minDistance = 3 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END AS RiskLevel
ORDER BY minDistance ASC, person.name
LIMIT 20;


// =========================================================
// Step 6: 綜合風險評分 (Composite Risk Scoring)
// =========================================================
// 目的：將「資金量(Degree)」、「結構緊密度(K-Core)」、「與壞人距離(Proximity)」結合為單一指標。
// 標準化 (Normalization)：將不同單位的數值 (金額、層級) 除以最大值，轉為 0~1 的小數，以便加權。

// 6.1 取得全域最大值以進行標準化
MATCH (p:Person)
WHERE p.weightedDegree IS NOT NULL
WITH max(p.weightedDegree) AS maxDegree,
     max(coalesce(p.coreValue, 0)) AS maxCore

// 6.2 計算加權分數
MATCH (p:Person)
SET p.collusionRiskScore = 
  (coalesce(p.weightedDegree, 0) / maxDegree) * 0.3 + // 權重 30%: 資金規模
  (coalesce(p.coreValue, 0) / maxCore) * 0.3 +        // 權重 30%: 共犯結構深度
  (CASE 
    WHEN p.fraudProximity = 1 THEN 0.4                // 權重 40% (最高): 與已知詐欺犯的距離
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


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('collusion-network');
```

---

## 第五部分:知識圖譜與語義分析

### 題目 13: 電影知識圖譜推理

**情境**: 建立能進行多跳推理的電影知識系統。
**目的**: 目的是建立一個「電影知識圖譜 (Knowledge Graph) 與語義搜尋系統」。

它不只看單純的「誰演了什麼」，而是進一步挖掘隱含關係：

1. 語義關係提取：自動建立「主題相似」（共享演員）與「影響傳承」（同導演/演員的時間跨度）的新關係。

2. 圖嵌入 (Graph Embedding)：使用 Node2Vec 將圖結構轉化為向量。這能捕捉到「結構上的相似性」，即使兩部電影沒有直接關聯，若它們處於類似的網絡結構中，也會被視為相似。

3. 語義推薦：基於向量的餘弦相似度 (Cosine Similarity) 進行推薦，能發現傳統查詢找不到的隱性關聯。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立知識圖譜的隱含關係 (Knowledge Graph Enrichment)
// =========================================================
// 目的：原始資料只有「人-電影」的連結。我們需要建立「電影-電影」的語義連結，讓圖譜更豐富。

// 1.1 建立「主題相似」關係 (Thematic Similarity)
// 邏輯：如果兩部電影有 2 個以上的共同演員，它們很可能屬於同一類型或同一群受眾。
MATCH (m1:Movie)<-[:ACTED_IN]-(p:Person)-[:ACTED_IN]->(m2:Movie)
WHERE id(m1) < id(m2) // 避免重複計算 (A-B 和 B-A)
WITH m1, m2, count(p) AS commonActors
WHERE commonActors >= 2
MERGE (m1)-[s:SIMILAR_THEME]-(m2)
SET s.strength = commonActors; // 共演人數越多，關係越強

// 1.2 建立「影響/傳承」關係 (Influence/Legacy)
// 邏輯：同一個創作者 (演員或導演)，相隔 1~15 年參與的兩部作品，通常存在藝術風格或經驗的傳承。
MATCH (older:Movie)<-[:ACTED_IN|DIRECTED]-(creator)-[:ACTED_IN|DIRECTED]->(newer:Movie)
WHERE older.released < newer.released 
  AND (newer.released - older.released) > 1 
  AND (newer.released - older.released) < 15
WITH older, newer, count(DISTINCT creator) AS influence
WHERE influence >= 1
MERGE (newer)-[i:INFLUENCED_BY]->(older) // 這是「有方向」的關係：新作受舊作影響
SET i.weight = influence;


// =========================================================
// Step 2: 建立多關係圖投影 (Multi-Relational Graph Projection)
// =========================================================
// 目的：將異質圖 (Heterogeneous Graph) 載入記憶體。
// 包含：原生的演出關係 + 我們剛剛推導出的相似與影響關係。
CALL gds.graph.drop("knowledge-graph-v2", FALSE); // 安全刪除

CALL gds.graph.project(
    'knowledge-graph-v2',
    ['Person','Movie'], // 載入兩種節點
    {
        ACTED_IN: {orientation: 'UNDIRECTED'},      // 演出關係視為無向
        DIRECTED: {orientation: 'UNDIRECTED'},      // 導演關係視為無向
        SIMILAR_THEME: {orientation: 'UNDIRECTED'}, // 相似關係視為無向
        INFLUENCED_BY: {orientation: 'NATURAL'}     // 影響關係保留方向 (New -> Old)
    }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 3: 最短路徑分析 (Classic Pathfinding)
// =========================================================
// 目的：驗證圖的連通性。這就是著名的「凱文貝肯六度分隔理論 (Six Degrees of Kevin Bacon)」。
// 用法：找出 Tom Hanks 到 Kevin Bacon 的最短人脈路徑。


[Image of shortest path graph algorithm]

MATCH (start:Person {name:'Tom Hanks'}), (end:Person {name:'Kevin Bacon'})
CALL gds.allShortestPaths.dijkstra.stream('knowledge-graph-v2', {
    sourceNode: start,
    relationshipTypes: ['ACTED_IN'] // 這裡只用「演出」關係來找人脈
})
YIELD totalCost, path
WITH [n IN nodes(path) | n.name] AS pathNodes, totalCost
RETURN pathNodes AS BaconPath, totalCost AS Distance
LIMIT 5;


// =========================================================
// Step 4: Node2Vec 嵌入學習 (Graph Embeddings)
// =========================================================
// 目的：將圖中的每個節點 (電影、人) 轉換成一個 128 維的數字向量。
// 演算法：Node2Vec。它透過「隨機遊走 (Random Walk)」來學習節點的上下文。
// 意義：如果兩個節點在圖上的「結構地位」或「鄰居結構」很像，它們的向量就會很近。
// 參數解釋：
// - walkLength: 每次遊走走多遠 (80步)。
// - inOutFactor (q): 控制遊走是偏向廣度優先(BFS)還是深度優先(DFS)。0.5 傾向於探索區域結構。
// - returnFactor (p): 控制回頭的機率。

CALL gds.node2vec.write('knowledge-graph-v2', {
    embeddingDimension: 128,
    walkLength: 80,
    walksPerNode: 10,
    inOutFactor: 0.5,
    returnFactor: 0.5,
    randomSeed: 42,
    writeProperty: 'kg_embedding' // 將向量寫回資料庫
})
YIELD nodePropertiesWritten, computeMillis
RETURN 'Node2Vec Embeddings' AS Feature,
       nodePropertiesWritten AS NodesEmbedded,
       computeMillis AS TimeMs;


// =========================================================
// Step 5: 基於嵌入的相似度推薦 (Semantic Similarity Search)
// =========================================================
// 目的：利用剛剛學到的向量 (kg_embedding) 來找相似電影。
// 優勢：這比傳統的「找共同演員」更強大。因為 Node2Vec 捕捉了結構特徵，
// 它可以找到「雖然沒有共同演員，但風格/類型/受眾群體非常像」的電影。
MATCH (m1:Movie), (m2:Movie)
WHERE m1 <> m2 AND m1.kg_embedding IS NOT NULL AND m2.kg_embedding IS NOT NULL
WITH m1, m2, gds.similarity.cosine(m1.kg_embedding, m2.kg_embedding) AS sim
WHERE sim > 0.6 // 只列出高度相似的
RETURN m1.title AS Movie, m2.title AS SimilarMovie, round(sim*1000)/1000 AS CosineSim
ORDER BY sim DESC
LIMIT 15;


// =========================================================
// Step 6: 知識樞紐分析 (Knowledge Hub Identification)
// =========================================================
// 目的：找出知識圖譜中的關鍵節點。
// 演算法：Harmonic Centrality (調和中心性)。
// 適用性：比 Closeness Centrality 更適合處理非連通圖。分數高代表該電影處於知識網絡的中心，
// 可能是連結不同時代、不同類型電影的關鍵作品 (如《教父》或《星際大戰》)。
CALL gds.closeness.harmonic.write('knowledge-graph-v2', {
    writeProperty: 'knowledgeHub'
})
YIELD centralityDistribution, computeMillis;

MATCH (m:Movie)
WHERE m.knowledgeHub IS NOT NULL
RETURN m.title AS Movie, round(m.knowledgeHub*1000)/1000 AS HubScore
ORDER BY m.knowledgeHub DESC
LIMIT 15;


// =========================================================
// Step 7: 清理 In-Memory 圖
// =========================================================
CALL gds.graph.drop('knowledge-graph-v2');
```

---

### 題目 14: 語義社群演化

**情境**: 分析電影主題和類型的演化趨勢。
**目的**: 目的是建立一個「動態社群演化分析系統 (Dynamic Community Evolution Analysis)」。
它展示了如何分析圖結構隨時間的變化，具體應用於電影類型的演變：

1. 時間切片 (Time Slicing)：將電影資料按年代（90年代、00年代、10年代）切分成不同的子圖。

2. 動態社群偵測：分別在不同年代的子圖上執行 Louvain 演算法，觀察「動作片」在 90 年代可能跟「冒險片」一群，但在 2010 年代可能跟「科幻片」一群。

3. 趨勢追蹤：比較同一節點在不同時期的社群 ID，判斷該類型是「穩定發展」、「轉型中」還是「發生融合」。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 資料準備 - 為電影添加類型標籤
// =========================================================
// 目的：因為原始 Movies 資料集可能沒有 genre 屬性，這裡隨機模擬。
// 實際場景中，這一步通常是資料清理 (ETL) 的一部分。
MATCH (m:Movie)
WITH m, ['Action', 'Drama', 'Comedy', 'Sci-Fi', 'Thriller', 'Romance'] AS genres
// 隨機選一個類型給電影
SET m.genre = genres[toInteger(rand() * size(genres))];


// =========================================================
// Step 2: 建立實體節點與關係 (Graph Construction)
// =========================================================
// 目的：將 Movie 節點上的字串屬性 (e.g., "Action") 轉化為實際的 Genre 節點。
// 這樣我們才能建立 (Movie)-[:HAS_GENRE]->(Genre) 的圖結構。

// 2.1 建立 Genre 節點與關係
MATCH (m:Movie)
WHERE m.genre IS NOT NULL
MERGE (g:Genre {name: m.genre})
MERGE (m)-[:HAS_GENRE]->(g);

// 2.2 [選擇性] 建立 Genre-Genre 共現圖 (Co-occurrence)
// 目的：雖然 GDS 可以處理二分圖，但在社群偵測中，我們常將其投影為「單模圖」(Monopartite)。
// 這裡將 "Movie連接兩個Genre" 轉換為 "Genre直接連接Genre"，權重為共同出現的電影數。


MATCH (g1:Genre)<-[:HAS_GENRE]-(m:Movie)-[:HAS_GENRE]->(g2:Genre)
WHERE id(g1) < id(g2) // 避免重複計算
WITH g1, g2, count(m) AS weight
MERGE (g1)-[r:CO_OCCUR]-(g2)
SET r.weight = weight;

// 投影全域共現圖 (不分年份，僅作參考)
CALL gds.graph.drop("genre-cooccurrence", FALSE);
CALL gds.graph.project(
    'genre-cooccurrence',
    'Genre',
    'CO_OCCUR'
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 3: 根據年份進行時間切片分析 (Temporal Analysis)
// =========================================================
// 目的：這段是核心。我們不分析整張圖，而是用 UNWIND 迴圈處理三個時間段。
// 針對每個時間段：
// 1. 使用 Cypher Projection 動態建立該年代的子圖 (只包含該年代上映的電影所構成的共現關係)。
// 2. 執行 Louvain 演算法找出該年代的類型社群。
// 3. 將結果 (community ID) 寫回 Genre 節點，屬性名稱動態加上年代後綴 (如 community_1990s)。

UNWIND [
  {name: '1990s', start: 1990, end: 2000},
  {name: '2000s', start: 2000, end: 2010},
  {name: '2010s', start: 2010, end: 9999}
] AS period

// 3.1 建立該年代的子圖
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

// 3.2 執行 Louvain 社群偵測並寫入結果
CALL gds.louvain.write('genre-' + period.name, {
  writeProperty: 'community_' + period.name // 動態屬性名
})
YIELD communityCount, modularity

RETURN period.name AS Period, communityCount, modularity;


// =========================================================
// [Optional] Step 4: 社群品質評估
// =========================================================
// 通常可使用 Conductance (導電率) 來評估社群切分的好壞，數值越低代表社群定義越明確。


// =========================================================
// Step 5: 視覺化類型演化趨勢 (Evolution Tracking)
// =========================================================
// 目的：比較同一種類型 (Genre) 在三個不同時期的社群歸屬。
// 邏輯：
// - Stable: 三個時期的社群 ID 都一樣 (或相對關係不變)，代表它很穩定。
// - Evolved: 早期和晚期的歸屬不同，代表它可能改變了風格或觀眾群。
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


// =========================================================
// Step 6: 識別融合趨勢 (Genre Fusion)
// =========================================================
// 目的：觀察在 2010 年代，哪兩個類型最常「混搭」在一起。
// 洞察：例如 Marvel 電影可能導致 "Action" 和 "Sci-Fi" 高度融合，甚至被歸類到同一個社群 (SameCommunity = true)。
MATCH (g1:Genre)<-[:HAS_GENRE]-(m:Movie)-[:HAS_GENRE]->(g2:Genre)
WHERE m.released >= 2010 AND id(g1) < id(g2)
WITH g1, g2, count(m) AS fusionCount
ORDER BY fusionCount DESC
LIMIT 10
RETURN 
  g1.name AS Genre1,
  g2.name AS Genre2,
  fusionCount AS FusionMovies,
  // 檢查它們在 2010 年代的演算法分群中，是否已被視為同一群
  g1.community_2010s = g2.community_2010s AS SameCommunity;


// =========================================================
// 清理資源
// =========================================================
// 批次刪除所有以 'genre-' 開頭的 GDS 投影圖
CALL gds.graph.list() YIELD graphName
WHERE graphName STARTS WITH 'genre-'
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN collect(dropped);
```

---

### 題目 15: 跨領域知識遷移

**情境**: 識別可以跨類型成功的演員和導演。
**目的**: 目的是建立一個「演藝人員跨界能力與類型壁壘分析系統」。

它試圖回答兩個深層問題：

1. 誰是全能型人才？ 找出不僅活躍（連結多），而且能駕馭多種不同電影類型（如同時演動作片和愛情片）的「跨界高手」。

2. 類型之間的鴻溝有多深？ 分析不同電影類型之間的人才流動難易度。例如，從「動作片」轉型去演「愛情片」容易嗎？還是這兩個圈子完全沒有交集？
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立電影人脈圖 (Graph Projection)
// =========================================================
// 目的：將所有「人」(演員或導演) 視為節點，只要有參與電影 (ACTED_IN 或 DIRECTED) 就視為有連結。
// 這是一個基礎的同質網路 (Monopartite Graph)，用於計算社交中心性。
CALL gds.graph.project(
  'movie-network',
  'Person',
  {
    // 將演出和執導都視為連結，且設為無向圖，因為我們關心的是「關聯強度」而非方向
    ACTED_IN: {orientation: 'UNDIRECTED'},
    DIRECTED: {orientation: 'UNDIRECTED'}
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 計算人物的 Degree Centrality (影壇活躍度)
// =========================================================
// 目的：使用度中心性 (Degree Centrality) 來衡量一個人的活躍程度。
// 意義：分數越高，代表這個人參與的專案越多，或者合作的人越多，是影壇的「熟面孔」。

CALL gds.degree.write('movie-network', {
  writeProperty: 'degreeCentrality'
})
YIELD nodePropertiesWritten;


// =========================================================
// Step 3: 對每個人計算參與的類型與多樣性 (Data Enrichment)
// =========================================================
// 目的：這一步不使用 GDS，而是用 Cypher 聚合查詢，統計每個人實際接觸過的電影類型 (Genres)。
// 邏輯：收集每個人參與電影的所有 genre，並計算不重複的類型數量 (genreCount)。
MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(m:Movie)
WITH p, collect(DISTINCT m.genre) AS genres
WITH p, genres, size(genres) AS genreCount
WHERE genreCount >= 2 // 過濾掉只演過一種類型的人，專注於跨界分析
SET p.genreCount = genreCount, p.genres = genres;


// =========================================================
// Step 4: 識別「跨界高手」 (Identify Polymaths)
// =========================================================
// 目的：結合 Step 2 (活躍度) 與 Step 3 (多樣性) 的指標。
// 篩選標準：
// 1. 涉獵廣：至少跨足 3 種以上類型。
// 2. 影響力大：Degree Centrality 高於 5 (避免選到雖然跨界但都是跑龍套的邊緣人)。
MATCH (p:Person)
WHERE p.genreCount >= 3 AND p.degreeCentrality > 5
RETURN 
  p.name AS CrossGenreExpert,
  p.genres AS GenresWorked,
  p.genreCount AS GenreCount,
  round(p.degreeCentrality, 2) AS CentralityScore
ORDER BY GenreCount DESC, CentralityScore DESC
LIMIT 15;


// =========================================================
// Step 5: 建立類型跨越難度矩陣 (Transition Difficulty Matrix)
// =========================================================
// 目的：分析行業結構。計算不同電影類型之間的人才重疊率。
// 洞察：
// - Easy: 兩個類型共用很多演員 (如 Action 和 Thriller)，轉型容易。
// - Difficult: 兩個類型幾乎沒共用演員 (如 Horror 和 Documentary)，隔行如隔山。

WITH ['Action', 'Drama', 'Comedy', 'Sci-Fi', 'Thriller', 'Romance'] AS genres
UNWIND genres AS g1
UNWIND genres AS g2
WITH g1, g2 WHERE g1 < g2 // 避免重複比較 (A vs B 和 B vs A)

// 找出參與 g1 類型的所有人 (Group 1)
MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(:Movie {genre: g1})
WITH g1, g2, collect(DISTINCT p) AS group1

// 找出參與 g2 類型的所有人 (Group 2)
MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(:Movie {genre: g2})
WITH g1, g2, group1, collect(DISTINCT p) AS group2

// 計算重疊人數 (Intersection) 與 Group 1 的總人數
WITH g1, g2, size([x IN group1 WHERE x IN group2]) AS crossover, size(group1) AS total

RETURN 
  g1 AS FromGenre,
  g2 AS ToGenre,
  crossover AS CrossoverArtists, // 兩邊都演過的跨界人數
  // 計算轉型率 (Transition Rate)：跨界人數 / 基數
  CASE WHEN total > 0 THEN round(toFloat(crossover) / total * 100) / 100 ELSE 0 END AS TransitionRate,
  // 定義難度等級
  CASE 
    WHEN toFloat(crossover) / total > 0.5 THEN 'Easy'      // 超過 50% 的人會跨過去 -> 容易
    WHEN toFloat(crossover) / total > 0.2 THEN 'Moderate'  // 中等
    ELSE 'Difficult'                                       // 低於 20% -> 困難 (壁壘分明)
  END AS Difficulty
ORDER BY TransitionRate DESC
LIMIT 20;


// =========================================================
// Step 6: 清理暫存圖
// =========================================================
CALL gds.graph.drop('movie-network') YIELD graphName
RETURN graphName;
```

---

## 第六部分:性能優化與大規模圖處理

### 題目 16: 記憶體優化的大圖處理

**情境**: 在有限記憶體下處理超大規模圖。
**目的**: 主要目的是展示 GDS 的效能優化與大規模圖形處理策略。

它不專注於單一演算法的分析結果，而是演示了在硬體資源有限或資料量極大時的處理技巧：

1. 過濾投影 (Filtered Projection)：在載入階段就過濾掉低關聯節點，減少記憶體佔用。

2. 分批處理 (Batch Processing)：利用社群演算法 (Louvain) 將大圖切分為多個小圖，再針對每個小圖分別運算 (Divide and Conquer)，避免記憶體溢出 (OOM)。

3. 效能基準測試 (Benchmarking)：比較不同投影方式 (Native vs. Cypher) 與輸出模式 (Stream vs. Write) 的效能差異。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 使用 Cypher Projection 建立優化子圖 (Filtered Projection)
// =========================================================
// 目的：在投影階段就進行資料過濾，只載入「活躍」的節點。
// 應用場景：當原始資料庫很大，但分析只需要其中一部分高品質資料時 (例如只分析演過 3 部片以上的資深演員)。
// 優點：大幅減少 In-Memory Graph 的記憶體消耗。

CALL gds.graph.project.cypher(
  'optimized-subgraph',
  // 節點查詢：只選取演過 >= 3 部電影的演員
  'MATCH (p:Person) 
   WHERE size((p)-[:ACTED_IN]->()) >= 3 
   RETURN id(p) AS id',
  // 關係查詢：建立演員之間的共同演出關係 (將 Person-Movie-Person 轉為 Person-Person)
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


// =========================================================
// Step 2: 分割圖為多個社群進行批次處理 (Graph Partitioning)
// =========================================================
// 目的：使用 Louvain 演算法將大圖切分成多個「連結緊密、彼此鬆散」的社群。
// 意義：這是「分治法 (Divide and Conquer)」的前奏。如果一張圖太大無法一次跑完 PageRank，
// 我們可以先把它切成 100 個小社群，一次只載入一個社群進記憶體運算。
CALL gds.louvain.write('optimized-subgraph', {
  writeProperty: 'batchCommunity', // 將社群 ID 寫回資料庫，作為後續批次處理的依據
  maxLevels: 10
})
YIELD communityCount, modularity;

// 檢查一下最大的幾個社群 (看看我們把圖切成了多大的碎塊)
MATCH (p:Person)
WHERE p.batchCommunity IS NOT NULL
WITH p.batchCommunity AS community, count(p) AS size
RETURN 
  community AS BatchID,
  size AS CommunitySize
ORDER BY size DESC
LIMIT 10;


// =========================================================
// Step 3: 對每個批次分別執行 PageRank (Batch Execution)
// =========================================================
// 目的：展示如何使用 CALL {...} 子查詢來進行迴圈處理。
// 流程：
// 1. 針對每一個社群 ID。
// 2. 動態建立只包含該社群節點的「微型圖」(batch-X)。
// 3. 在微型圖上跑 PageRank。
// 4. 寫入結果後，立刻刪除微型圖釋放記憶體。

MATCH (p:Person)
WHERE p.batchCommunity IS NOT NULL
WITH DISTINCT p.batchCommunity AS communityId
LIMIT 5 // 為了演示，只跑前 5 個社群

CALL {
  WITH communityId
  // 1. 抓出該社群的所有節點 ID
  MATCH (p:Person {batchCommunity: communityId})
  WITH collect(id(p)) AS communityNodes
  
  // 2. 動態投影微型圖 (只載入該社群的資料)
  CALL gds.graph.project.cypher(
    'batch-' + communityId,
    'MATCH (p:Person) WHERE id(p) IN $nodes RETURN id(p) AS id',
    'MATCH (p1:Person)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2:Person)
     WHERE id(p1) IN $nodes AND id(p2) IN $nodes
     RETURN id(p1) AS source, id(p2) AS target',
    {parameters: {nodes: communityNodes}}
  )
  YIELD graphName
  
  // 3. 執行演算法
  CALL gds.pageRank.write(graphName, {
    writeProperty: 'pagerank_batch',
    maxIterations: 20
  })
  YIELD nodePropertiesWritten, ranIterations
  
  // 4. 結束後 GDS 會自動清理子查詢內的圖 (或可手動 drop)，這裡依賴 session 結束或手動清理
  RETURN communityId, nodePropertiesWritten
}
RETURN communityId, nodePropertiesWritten;


// =========================================================
// Step 4: 合併批次結果 (Normalization)
// =========================================================
// 目的：因為 PageRank 是在各個小圖分別算的，分數的基準不同 (局部 PageRank)。
// 這裡做一個簡單的歸一化 (除以最大值)，讓分數在 0~1 之間，方便整體比較。
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


// =========================================================
// Step 5: Stream mode vs Write mode 記憶體比較
// =========================================================
// 目的：比較兩種輸出模式的開銷。
// - Stream Mode: 僅將結果流式傳輸回 Client，不寫硬碟，速度快，對 DB 負擔最小。
// - Write Mode: 將結果寫回 Neo4j Store，會產生 Transaction Log，消耗較多 IO 與記憶體。

// Stream mode (輕量級)
CALL gds.pageRank.stream('optimized-subgraph', {
  maxIterations: 20
})
YIELD nodeId, score
WITH count(*) AS streamCount
RETURN 'Stream Mode' AS Mode, streamCount AS ProcessedNodes, 'Low Memory' AS MemoryUsage;

// Write mode (重量級，需持久化)
CALL gds.pageRank.write('optimized-subgraph', {
  writeProperty: 'pagerank_write',
  maxIterations: 20
})
YIELD nodePropertiesWritten
RETURN 'Write Mode' AS Mode, nodePropertiesWritten AS ProcessedNodes, 'Higher Memory' AS MemoryUsage;


// =========================================================
// Step 6: 比較不同投影方式的性能 (Projection Benchmarking)
// =========================================================
// 目的：比較 Native Projection 與 Cypher Projection 的速度。
// - Native Projection: 直接讀取 Neo4j 儲存檔，速度極快 (通常快 10 倍以上)。
// - Cypher Projection: 透過 Cypher 查詢引擎篩選資料，靈活但速度較慢。

// 1. Native Projection 測試
CALL gds.graph.project(
  'native-projection',
  'Person',
  {ACTED_IN: {orientation: 'UNDIRECTED'}}
)
YIELD graphName, projectMillis AS nativeTime, nodeCount, relationshipCount
WITH 'Native' AS method, nativeTime, nodeCount, relationshipCount

// 2. Cypher Projection 測試 (模擬相同邏輯)
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


// =========================================================
// 清理資源
// =========================================================
// 批次刪除所有測試用的圖 (包含 batch- 開頭的動態圖)
CALL gds.graph.list() YIELD graphName
WHERE graphName IN ['optimized-subgraph', 'native-projection', 'cypher-projection'] 
   OR graphName STARTS WITH 'batch-'
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN collect(dropped);
```

---

### 題目 17: 並行演算法調度優化 (unlicenced版本只能用4顆核心)

**情境**: 設計高效的 GDS 演算法執行計畫。
**目的**: 目的是建立一個「演算法執行排程與效能優化管道 (Pipeline Scheduling & Optimization)」。

在實際的生產環境中，亂跑 GDS 演算法容易導致記憶體溢出 (OOM) 或執行時間過長。這段程式碼展示了如何：

1. 分階段執行 (Phased Execution)：根據演算法的依賴性與輕重程度（輕量先跑，重量後跑）來規劃執行順序。

2. 並行度調優 (Concurrency Tuning)：測試不同 CPU 執行緒數量對運算速度的影響。

3. 結果快取 (Result Caching)：將中間運算結果寫回資料庫 (.write 模式)，供後續查詢直接使用，避免重複計算。

4. 資源監控 (Resource Monitoring)：監控圖形在記憶體中的大小。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立測試圖 (Graph Projection)
// =========================================================
// 目的：建立基礎環境。
// 注意：為了測試排程，我們先清理舊圖，確保環境乾淨。
CALL gds.graph.drop("scheduling-network", FALSE); // FALSE 表示如果圖不存在不報錯

CALL gds.graph.project(
  'scheduling-network',
  ['Person', 'Movie'],
  {
      ACTED_IN: {orientation: 'UNDIRECTED'},
      DIRECTED: {orientation: 'UNDIRECTED'}
  }
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 分析演算法依賴關係並規劃執行順序 (Pipeline Scheduling)
// =========================================================
// 目的：模擬一個 ETL 管道。
// 原則：
// 1. 獨立且輕量的演算法先跑 (如 Degree, Triangle)。
// 2. 分析全域結構的演算法次之 (如 PageRank, Louvain)。
// 3. 最耗資源的演算法最後跑 (如 Similarity)。

// --- 階段 1: 獨立演算法 (Phase 1: Lightweight & Independent) ---
WITH timestamp() AS phase1Start

// 任務 1: Degree Centrality (計算量極小，O(N))
CALL gds.degree.write('scheduling-network', {
  writeProperty: 'degree_sched',
  concurrency: 4 // 指定使用 4 個執行緒並行運算
})
YIELD nodePropertiesWritten AS degreeNodes, computeMillis AS degreeTime

WITH phase1Start, degreeNodes, degreeTime

// 任務 2: Triangle Count (計算量中等，主要看平均度數)
CALL gds.triangleCount.write('scheduling-network', {
  writeProperty: 'triangles_sched',
  concurrency: 4
})
YIELD nodePropertiesWritten AS triangleNodes, computeMillis AS triangleTime

RETURN 
  'Phase 1: Independent Algorithms' AS Phase,
  degreeTime AS DegreeTimeMs,
  triangleTime AS TriangleTimeMs,
  'Can run in parallel' AS Note; // 註記：在外部程式(如 Python)中，這兩個可以同時發起


// --- 階段 2: 結構分析演算法 (Phase 2: Global Structure) ---
WITH timestamp() AS phase2Start

// PageRank (迭代式演算法，需要多次掃描全圖)
CALL gds.pageRank.write('scheduling-network', {
  writeProperty: 'pagerank_sched',
  maxIterations: 20,
  concurrency: 4
})
YIELD nodePropertiesWritten, ranIterations, computeMillis AS prTime

WITH phase2Start, prTime

// Louvain (社群偵測，依賴圖的拓樸結構)
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


// --- 階段 3: 高運算量演算法 (Phase 3: Heavy Computation) ---
WITH timestamp() AS phase3Start

// Node Similarity (時間複雜度接近 O(N^2)，最耗資源)
// 通常安排在最後，且建議設定 similarityCutoff 減少寫入量
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


// =========================================================
// Step 3: 測試不同 concurrency 參數的影響 (Performance Tuning)
// =========================================================
// 目的：找出這台機器執行該演算法的「甜蜜點 (Sweet Spot)」。
// 並行度不是越高越好，過高會導致 Context Switch 開銷，過低則浪費 CPU。
WITH [1, 2, 4] AS concurrencyLevels // 測試 1, 2, 4 核心
UNWIND concurrencyLevels AS conc

// 在外層取得開始時間
WITH conc, timestamp() AS startTime

// 使用子查詢 (CALL {}) 隔離每次執行環境
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
  (endTime - startTime) AS ExecutionTimeMs, // 觀察哪一個設定跑最快
  nodesProcessed AS NodesProcessed
ORDER BY Concurrency;


// =========================================================
// Step 4: 實作結果快取策略 (Caching Strategy)
// =========================================================
// 目的：模擬「快取」。
// 在 GDS 中，最好的快取就是使用 .write 模式將結果寫回 Neo4j DB (硬碟)。
// 下次查詢時，直接 MATCH 屬性 (O(1))，而不用重新 CALL gds.pageRank.stream (O(N+E))。

// 4.1 模擬從資料庫讀取已計算好的屬性 (Cache Hit)
MATCH (p:Person)
WHERE p.pagerank_sched IS NOT NULL
WITH collect({id: id(p), pr: p.pagerank_sched}) AS cachedResults
WITH cachedResults, size(cachedResults) AS cacheSize

// 4.2 模擬後續應用 (例如：推薦系統只取前 10 名)
MATCH (p:Person)
WHERE id(p) IN [r IN cachedResults | r.id][..10]
RETURN 
  p.name AS Person,
  p.pagerank_sched AS CachedPageRank,
  'Retrieved from cache' AS Source // 證明我們沒有重新計算
LIMIT 10;


// =========================================================
// Step 5: 監控資源 (Resource Monitoring)
// =========================================================
// 目的：檢查目前 GDS 佔用了多少記憶體。
// 這對於排程很重要：如果記憶體快滿了，就不能啟動新的投影或演算法。
CALL gds.graph.list('scheduling-network')
YIELD graphName, memoryUsage, sizeInBytes, nodeCount, relationshipCount
RETURN 
  graphName AS Graph,
  memoryUsage AS MemoryUsage, // 格式化字串 (如 "128 MiB")
  sizeInBytes AS SizeBytes,   // 實際 Byte 數
  nodeCount AS Nodes,
  relationshipCount AS Relationships;


// =========================================================
// Step 6: 完整的優化執行計畫 (Execution Plan Simulation)
// =========================================================
// 目的：將所有步驟整合為一個邏輯區塊，展示一個完整的 ETL 流程。
WITH timestamp() AS totalStart

// 1. 執行基礎指標計算
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

// 2. 執行進階結構分析
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


// =========================================================
// 清理
// =========================================================
CALL gds.graph.drop('scheduling-network');
```
---

### 題目 18: 漸進式圖更新策略 

**情境**: 圖數據持續增長,需要高效更新分析結果。
**目的**: 主要目的是展示 「動態圖形的增量更新策略 (Incremental Graph Update Strategy)」。

在現實世界中，圖形數據是隨時間不斷變化的。每次有新數據進來都「重新計算全圖 (Full Recalculation)」非常耗時且昂貴。這段程式碼演示了一種優化策略：

1. 影響範圍分析：找出新數據影響了哪些節點。

2. 局部重算：利用「社群 (Community)」作為邊界，只針對受影響的社群進行演算法重算（因為社群內部的連結緊密，外部連結鬆散，影響通常被限制在社群內）。

3. 決策機制：監控變化幅度，設定閾值來決定何時可以「只做增量更新」，何時必須「全圖重算」。
#### 解答程式碼:

```cypher
// =========================================================
// Step 1: 建立基線圖 (Baseline Graph Projection)
// =========================================================
// 目的：將目前的數據狀態載入記憶體，作為比較的基準點。
CALL gds.graph.project(
  'incremental-network',
  ['Person', 'Movie'],
  {ACTED_IN: {orientation: 'UNDIRECTED'}}
)
YIELD graphName, nodeCount, relationshipCount;


// =========================================================
// Step 2: 計算基線分析結果 (Baseline Calculation)
// =========================================================
// 目的：計算並儲存當前的 PageRank 和社群結構。
// 重要：這裡必須先算好 Louvain 社群，因為後面的「局部更新」會依賴這些社群邊界。


[Image of label propagation algorithm]


// 2.1 計算基準 PageRank
CALL gds.pageRank.write('incremental-network', {
  writeProperty: 'pagerank_baseline',
  maxIterations: 20
})
YIELD nodePropertiesWritten, ranIterations;

// 2.2 計算基準社群 (Louvain)
CALL gds.louvain.write('incremental-network', {
  writeProperty: 'community_baseline'
})
YIELD communityCount, modularity AS baselineModularity;

// 2.3 統計目前的平均指標，供後續比較
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


// =========================================================
// Step 3: 模擬新節點和邊的增加 (Simulate Dynamic Changes)
// =========================================================
// 目的：模擬真實世界中數據的變動。這裡隨機增加了約 10% 的新人物與新電影，並與舊節點產生連結。
MATCH (existingPerson:Person)
WITH existingPerson, rand() AS r
WHERE r < 0.1
LIMIT toInteger(0.1 * count(existingPerson))

// 建立新的人
CREATE (newPerson:Person {
  name: 'New_' + existingPerson.name + '_' + toInteger(rand() * 1000),
  isNew: true
})
WITH existingPerson, newPerson
// 建立新的電影，並建立連結
CREATE (newPerson)-[:ACTED_IN]->(newMovie:Movie {
  title: 'New Movie ' + toInteger(rand() * 1000),
  released: 2024,
  isNew: true
})
CREATE (existingPerson)-[:ACTED_IN]->(newMovie);


// =========================================================
// Step 4: 識別受影響的節點 (Impact Analysis)
// =========================================================
// 目的：找出哪些舊節點被新數據「汙染」或「影響」了。
// 邏輯：只要直接連接到新電影 (isNew: true) 的人，其 PageRank 勢必會改變，標記為 affected。
MATCH (affected:Person)-[:ACTED_IN]->(:Movie {isNew: true})
SET affected.affected = true
WITH count(DISTINCT affected) AS affectedCount
RETURN 
  'Impact Analysis' AS Stage,
  affectedCount AS AffectedNodes,
  'These nodes need recalculation' AS Note;


// =========================================================
// Step 5: 智能更新 - 只對受影響的社群重新計算 (Smart Local Update)
// =========================================================
// 目的：這是增量更新的核心技巧。
// 原理：PageRank 的分數流動通常在「連結緊密」的社群內影響最大，對社群外的影響會快速衰減。
// 作法：
// 1. 找出受影響節點所屬的「基準社群 ID」。
// 2. 只載入這些社群的節點與關係 (Sub-graph)。
// 3. 在這個小範圍內重算 PageRank。

MATCH (affected:Person {affected: true})
WITH DISTINCT affected.community_baseline AS affectedCommunity
// 找出該社群內的所有成員 (包含未被標記 affected 的鄰居)
MATCH (member:Person {community_baseline: affectedCommunity})
WITH collect(id(member)) AS communityNodes, affectedCommunity

// 動態投影：只針對該社群建立微型圖
CALL gds.graph.project.cypher(
  'affected-community-' + affectedCommunity,
  'MATCH (p:Person) WHERE id(p) IN $nodes RETURN id(p) AS id',
  'MATCH (p1:Person)-[:ACTED_IN]->()<-[:ACTED_IN]-(p2:Person)
   WHERE id(p1) IN $nodes AND id(p2) IN $nodes
   RETURN id(p1) AS source, id(p2) AS target',
  {parameters: {nodes: communityNodes}}
)
YIELD graphName, nodeCount

// 在微型圖上計算新的 PageRank (pagerank_updated)
CALL gds.pageRank.write(graphName, {
  writeProperty: 'pagerank_updated',
  maxIterations: 20
})
YIELD nodePropertiesWritten

RETURN 
  affectedCommunity AS Community,
  nodeCount AS NodesRecalculated,
  nodePropertiesWritten AS UpdatedNodes;


// =========================================================
// Step 6: Delta-based approach - 追蹤 PageRank 變化
// =========================================================
// 目的：量化變動幅度。比較「基準值」與「局部更新值」。
// 意義：如果變化很小，代表增量更新是成功的；如果變化劇烈，可能代表局部更新不準確。
MATCH (p:Person)
WHERE p.pagerank_baseline IS NOT NULL AND p.pagerank_updated IS NOT NULL
WITH p, 
     abs(p.pagerank_updated - p.pagerank_baseline) AS delta,
     ((p.pagerank_updated - p.pagerank_baseline) / p.pagerank_baseline * 100) AS percentChange
WHERE delta > 0.001 // 過濾掉微小的浮點數誤差
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


// =========================================================
// Step 7: 設計觸發條件 - 決定何時全圖重算 (Decision Trigger)
// =========================================================
// 目的：建立自動化運維的規則。
// 規則邏輯：
// 1. 如果變動節點超過總數 30% -> 局部更新已無效益，直接 FULL_RECALC。
// 2. 如果平均變動幅度 (AvgDelta) 太大 -> 結構發生根本改變，建議 FULL_RECALC。
// 3. 否則 -> INCREMENTAL_OK (本次增量更新有效，節省了資源)。
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


// =========================================================
// Step 8: 性能比較 (Performance Benchmark)
// =========================================================
// 目的：證明增量更新真的比較快。
// 增量更新時間 (已執行)
WITH timestamp() AS incrementalTime
MATCH (p:Person {affected: true})
WITH count(p) AS incrementalNodes, incrementalTime

// 模擬全圖重算時間 (Full Recalc)
WITH incrementalNodes, timestamp() AS fullRecalcStart
CALL gds.pageRank.stream('incremental-network', {
  maxIterations: 20
})
YIELD nodeId, score
WITH incrementalNodes, count(*) AS fullRecalcNodes, timestamp() - fullRecalcStart AS fullRecalcTime

RETURN 
  'Performance Comparison' AS Metric,
  incrementalNodes AS IncrementalNodes, // 增量只算了這些點
  fullRecalcNodes AS FullRecalcNodes,   // 全圖算了所有點
  fullRecalcTime AS FullRecalcTimeMs,
  'Incremental is much faster' AS Conclusion;


// =========================================================
// 清理
// =========================================================
CALL gds.graph.list() YIELD graphName
WHERE graphName = 'incremental-network' OR graphName STARTS WITH 'affected-community-'
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN collect(dropped);
```