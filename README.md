Запуск в три этапа: documents_counter, skiplist_builder, подставляем полученные константы в index_builder и запускаем его.

hadoop jar {jar} wiki.Runner {input} {output[ignored]}

documents_counter - все очень тупо, подсчет количества документов: {docid, text} -M-> {1, 1} -R-> {documents_count}

skiplist_builder - построение списка игнорируемых слов:

* {docid, text} -M-> {word, 1} -R-> {word, count} (посчитали)

* {word, count} -M-> {-count, word} -R-> {word, count} (отсортировали)

После подставляем константы (ничего лучше не придумал, но наверное можно как-то выкрутиться с помощью hdfs)

index_builder - построение индекса

* {docid, text} -M-> {word@docid, 1} -R-> {word@docid, count} (посчитали вхождения слова в каждый текст)

* {word@docid, count} -M-> {docid, word@count} -R-> {word@docid, count@total} (посчитали, сколько всего слов в каждом документе)

* {word@docid, count@total} -M-> {word, docid@count@total} -R-> (word, index} (в редьюсере сначала посчитали количество документов с этим словом, после этого посчитали все метрики и посортировали результаты по убыванию)
