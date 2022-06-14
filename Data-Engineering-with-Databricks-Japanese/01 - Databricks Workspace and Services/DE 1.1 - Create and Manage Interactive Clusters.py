# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # インタラクティブクラスタの作成と管理（Create and Manage Interactive Clusters）
# MAGIC 
# MAGIC Databricksのクラスタはコンピュートリソースと構成のセットで、その上で本番ETLパイプラインやストリーミング分析、アドホック分析、および機械学習など、データエンジニアリング、データサイエンス、データ分析のワークロードが実行されます。 これらのワークロードをノートブックのコマンドのセット、または自動化されたジョブとして実行します。
# MAGIC 
# MAGIC Databricksは、汎用クラスタとジョブクラスタを区別しています。
# MAGIC - 汎用クラスタはインタラクティブなノートブックを使用して、共同作業でデータを分析します。
# MAGIC - ジョブクラスタは、自動化された迅速かつ堅牢なジョブを実行するために使用します。
# MAGIC 
# MAGIC このデモでは、Databricksデータサイエンスとエンジニアリングワークスペースを使用した汎用Databricksクラスタの作成と管理について説明します。
# MAGIC 
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * クラスタUIを使用して、クラスタの構成およびデプロイを行う
# MAGIC * クラスタの編集、終了、再起動、削除をする

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## クラスタを作成する（Create Cluster）
# MAGIC 
# MAGIC 現在作業しているワークスペースによっては、クラスタの作成権限がある場合とない場合があります。
# MAGIC 
# MAGIC このセクションの説明は、クラスタ作成権限が**あり**、このコースのレッスンを実行するには新しいクラスタをデプロイする必要があることを前提としています。
# MAGIC 
# MAGIC **注**：インストラクターまたはプラットフォーム管理者に問い合わせて、新しいクラスタを作成するか、すでにデプロイされているクラスタに接続する必要があるかどうかを確認してください。 クラスタポリシーは、クラスタ構成のオプションに影響する可能性があります。
# MAGIC 
# MAGIC 手順は、次の通りです。
# MAGIC 1. 左サイドバーにある![compute](https://files.training.databricks.com/images/clusters-icon.png)アイコンをクリックして**コンピュート**ページに移動します
# MAGIC 1. 青色の**クラスタを作成**ボタンをクリックします
# MAGIC 1. **クラスタ名**については、簡単に見つけられて問題が発生した場合にインストラクターが簡単に識別できるよう、あなたの名前を使ってください
# MAGIC 1. **クラスタモード**を**シングルノード**に設定します（このコースを実行するにはこのモードが必要です）
# MAGIC 1. このコースでは、推奨されている**Databricksランタイムバージョン**を使用してください
# MAGIC 1. **オートパイロットオプション**の下にあるデフォルト設定のチェックボックスにチェックを入れたままにします
# MAGIC 1. 青色の**クラスタを作成**ボタンをクリックします
# MAGIC 
# MAGIC **注**：クラスタのデプロイには数分かかる場合があります。 クラスタのデプロイが完了したら、引き続きクラスタ作成UIを自由にさわってみてください。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## クラスタの管理（Manage Clusters）
# MAGIC 
# MAGIC クラスタが作成されたら、 **コンピュート**ページに戻ってクラスタを表示してください。
# MAGIC 
# MAGIC クラスタを選択して、現在の構成を確認します。
# MAGIC 
# MAGIC **編集**ボタンをクリックします。 設定のほとんどは変更できることに注意してください（十分な権限がある場合）。 ほとんどの設定の変更には、実行中のクラスタを再起動する必要があります。
# MAGIC 
# MAGIC **注**：次のレッスンではクラスタを使用します。 クラスタを再起動、終了、または削除すると、新しいリソースがデプロイされるのを待つ際に遅れが生じる可能性があります。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 再起動、終了、削除（Restart, Terminate, and Delete）
# MAGIC 
# MAGIC **再起動**、**終了**、**削除**には異なる効果があり、いずれもクラスタ終了イベントで開始することに注意してください。 （この設定が使用されている場合、クラスタも非アクティブなため、自動的に終了します。）
# MAGIC 
# MAGIC クラスタが終了すると、現在使用中のすべてのクラウドリソースが削除されます。 これは次のことを意味します：
# MAGIC * 関連する仮想マシンとオペレーショナルメモリがパージされます
# MAGIC * アタッチされたボリュームストレージが削除されます
# MAGIC * ノード間のネットワーク接続が解除されます
# MAGIC 
# MAGIC つまり、以前、コンピュート環境に関連していたリソースはすべて完全に削除されます。 これは**永続化する必要のある結果はいずれも永続的な場所に保存する必要がある**ことを意味します。 なお、コードが失われたり、適切に保存したデータファイルが失われたりすることはありません。
# MAGIC 
# MAGIC **再起動**ボタンを使用すると、クラスタを手動で再起動できます。 これは、クラスタのキャッシュを完全に消去する必要がある場合や、コンピュート環境を完全にリセットする必要がある場合に役立ちます。
# MAGIC 
# MAGIC **終了**ボタンを使用すると、クラスタを停止できます。 クラスタ構成の設定は維持されているので、**再起動**ボタンを使って同じ構成を使用した新しいクラウドリソースセットをデプロイできます。
# MAGIC 
# MAGIC **削除**ボタンは、クラスタを停止し、クラスタ構成を削除します。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
