// Copyright 2025 Сергей Савельев (serge@savel.pro)
//
// Лицензировано согласно Лицензии Apache, Версия 2.0 ("Лицензия");
// вы можете использовать этот файл только в соответствии с Лицензией.
// Вы можете найти копию Лицензии по адресу
//
// http://www.apache.org/licenses/LICENSE-2.0.
//
// За исключением случаев, когда это регламентировано существующим
// законодательством, или если это не оговорено в письменном соглашении,
// программное обеспечение, распространяемое на условиях данной Лицензии,
// предоставляется "КАК ЕСТЬ", и любые явные или неявные ГАРАНТИИ ОТВЕРГАЮТСЯ.
// Информацию об основных правах и ограничениях, применяемых к определенному
// языку согласно Лицензии, вы можете найти в данной Лицензии.

///////////////////////////////////////////////////////////////////////////////
// Низкоуровневый API интеграции Apache Kafka и 1C:Предприятие через HTTP-шлюз.
// Автор: Сергей Савельев (serge@savel.pro).
// Репозиторий HTTP-шлюза:
// Основной: https://github.com/SergeSavel/kafka-gateway
// Зеркало:  https://gitflic.ru/project/sergesavel/kafka-gateway
///////////////////////////////////////////////////////////////////////////////

#Область Переменные

Перем HttpСоединение;
Перем ПараметрыЗаписиJson;

#КонецОбласти

#Область Инициализация

Процедура Инициализировать(ШлюзАдрес = Неопределено, ШлюзПользователь = Неопределено, ШлюзПароль = Неопределено, ШлюзТаймаут = 65, ЗащищенноеСоединение = Неопределено) Экспорт
		
	Если ШлюзАдрес = Неопределено Или ПустаяСтрока(ШлюзАдрес) Тогда
		ШлюзСервер = "localhost";
		ШлюзПорт = 8086;
	Иначе
		ПозДвоеточие = СтрНайти(ШлюзАдрес, ":");
		Если ПозДвоеточие = 0 Тогда
			ШлюзСервер = ШлюзАдрес;
			ШлюзПорт = 8086;
		Иначе
			ШлюзСервер = Лев(ШлюзАдрес, ПозДвоеточие-1);
			ШлюзПорт = Число(Сред(ШлюзАдрес, ПозДвоеточие+1));
		КонецЕсли;
	КонецЕсли;
	
	Если ЗащищенноеСоединение = Неопределено Тогда	
		HttpСоединение = Новый HttpСоединение(ШлюзСервер, ШлюзПорт, ШлюзПользователь, ШлюзПароль, , ШлюзТаймаут);
	Иначе
		HttpСоединение = Новый HttpСоединение(ШлюзСервер, ШлюзПорт, ШлюзПользователь, ШлюзПароль, , ШлюзТаймаут, ЗащищенноеСоединение);
	КонецЕсли;
	
	ПараметрыЗаписиJson = Новый ПараметрыЗаписиJson(ПереносСтрокJson.Нет);
		
КонецПроцедуры

#КонецОбласти

#Область Отправка

Функция ProducerCreate(Name, Config, Знач ExpirationTimeout = Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	ProducerCreateRequest = Новый Структура;
	ProducerCreateRequest.Вставить("name", Name);
	ProducerCreateRequest.Вставить("config", Config);
	ProducerCreateRequest.Вставить("expirationTimeout", ExpirationTimeout);
	
	HttpОтвет = ВыполнитьPost("producer/create", ProducerCreateRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);;
	
КонецФункции

Функция ProducerRelease(Producer) Экспорт
	
	ProducerReleaseRequest = Новый Структура;
	ProducerReleaseRequest.Вставить("producerId", Producer.id);
	ProducerReleaseRequest.Вставить("token", Producer.token);
	
	HttpОтвет = ВыполнитьPost("producer/release", ProducerReleaseRequest);
	
	Возврат (HttpОтвет <> Неопределено);
	
КонецФункции

Функция ProducerList() Экспорт
	
	HttpОтвет = ВыполнитьGet("producer");
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ProducerTouch(Producer) Экспорт
	
	ProducerTouchRequest = Новый Структура;
	ProducerTouchRequest.Вставить("producerId", Producer.id);
	ProducerTouchRequest.Вставить("token", Producer.token);
	
	HttpОтвет = ВыполнитьPost("producer/touch", ProducerTouchRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ProducerGetPartitions(Producer, Topic) Экспорт
	
	ProducerGetPartitionsRequest = Новый Структура;
	ProducerGetPartitionsRequest.Вставить("producerId", Producer.id);
	ProducerGetPartitionsRequest.Вставить("token", Producer.token);
	ProducerGetPartitionsRequest.Вставить("topic", Topic);
	
	HttpОтвет = ВыполнитьPost("producer/get-partitions", ProducerGetPartitionsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ProducerSend(Producer, Topic, KeyString = Неопределено, ValueString, Headers = Неопределено) Экспорт
	
	ProducerSendStringRequest = Новый Структура;
	ProducerSendStringRequest.Вставить("producerId", Producer.id);
	ProducerSendStringRequest.Вставить("token", Producer.token);
	ProducerSendStringRequest.Вставить("topic", Topic);
	Если Headers <> Неопределено Тогда
		ProducerSendStringRequest.Вставить("headers", Headers);
	КонецЕсли;
	Если KeyString <> Неопределено Тогда
		ProducerSendStringRequest.Вставить("key", KeyString);
	КонецЕсли;
	ProducerSendStringRequest.Вставить("value", ValueString);
	
	HttpОтвет = ВыполнитьPost("producer/send", ProducerSendStringRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ProducerBeginTransaction(Producer) Экспорт
	
	ProducerBeginTransactionRequest = Новый Структура;
	ProducerBeginTransactionRequest.Вставить("producerId", Producer.id);
	ProducerBeginTransactionRequest.Вставить("token", Producer.token);
	
	HttpОтвет = ВыполнитьPost("producer/begin-transaction", ProducerBeginTransactionRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ProducerCommitTransaction(Producer) Экспорт
	
	ProducerCommitTransactionRequest = Новый Структура;
	ProducerCommitTransactionRequest.Вставить("producerId", Producer.id);
	ProducerCommitTransactionRequest.Вставить("token", Producer.token);
	
	HttpОтвет = ВыполнитьPost("producer/commit-transaction", ProducerCommitTransactionRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ProducerAbortTransaction(Producer) Экспорт
	
	ProducerAbortTransactionRequest = Новый Структура;
	ProducerAbortTransactionRequest.Вставить("producerId", Producer.id);
	ProducerAbortTransactionRequest.Вставить("token", Producer.token);
	
	HttpОтвет = ВыполнитьPost("producer/abort-transaction", ProducerAbortTransactionRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

#КонецОбласти

#Область Получение

Функция ConsumerCreate(Name, Config, Знач ExpirationTimeout = Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	ConsumerCreateRequest = Новый Структура;
	ConsumerCreateRequest.Вставить("name", Name);
	ConsumerCreateRequest.Вставить("config", Config);
	ConsumerCreateRequest.Вставить("expirationTimeout", ExpirationTimeout);
	
	HttpОтвет = ВыполнитьPost("consumer/create", ConsumerCreateRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerRelease(Consumer) Экспорт
			
	ConsumerReleaseRequest = Новый Структура;
	ConsumerReleaseRequest.Вставить("consumerId", Consumer.id);
	ConsumerReleaseRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/release", ConsumerReleaseRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerList() Экспорт
	
	HttpОтвет = ВыполнитьGet("consumer");
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerTouch(Consumer) Экспорт
	
	ConsumerTouchRequest = Новый Структура;
	ConsumerTouchRequest.Вставить("consumerId", Consumer.id);
	ConsumerTouchRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/touch", ConsumerTouchRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerListTopics(Consumer, Pattern = Неопределено) Экспорт
		
	ConsumerListTopicsRequest = Новый Структура;
	ConsumerListTopicsRequest.Вставить("consumerId", Consumer.id);
	ConsumerListTopicsRequest.Вставить("token", Consumer.token);
	Если Pattern <> Неопределено Тогда
		ConsumerListTopicsRequest.Вставить("pattern", Pattern);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("consumer/list-topics", ConsumerListTopicsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerGetPartitions(Consumer, Topic) Экспорт
		
	ConsumerGetPartitionsRequest = Новый Структура;
	ConsumerGetPartitionsRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetPartitionsRequest.Вставить("token", Consumer.token);
	ConsumerGetPartitionsRequest.Вставить("topic", Topic);
	
	HttpОтвет = ВыполнитьPost("consumer/get-partitions", ConsumerGetPartitionsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerAssign(Consumer, Partitions) Экспорт
	
	ConsumerAssignRequest = Новый Структура;
	ConsumerAssignRequest.Вставить("consumerId", Consumer.id);
	ConsumerAssignRequest.Вставить("token", Consumer.token);
	ConsumerAssignRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("consumer/assign", ConsumerAssignRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerGetAssignment(Consumer) Экспорт
		
	ConsumerGetAssignmentRequest = Новый Структура;
	ConsumerGetAssignmentRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetAssignmentRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/get-assignment", ConsumerGetAssignmentRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerSubscribe(Consumer, TopicsOrPattern) Экспорт
	
	ТипTopicsOrPattern = ТипЗнч(TopicsOrPattern);
	
	ConsumerSubscribeRequest = Новый Структура;
	ConsumerSubscribeRequest.Вставить("consumerId", Consumer.id);
	ConsumerSubscribeRequest.Вставить("token", Consumer.token);
	Если ТипTopicsOrPattern = Тип("Массив") Или ТипTopicsOrPattern = Тип("ФиксированныйМассив") Тогда
		ConsumerSubscribeRequest.Вставить("topics", TopicsOrPattern);
	ИначеЕсли ТипTopicsOrPattern = Тип("Строка") Тогда
		ConsumerSubscribeRequest.Вставить("pattern", TopicsOrPattern);
	Иначе
		ВызватьИсключение "Некорректный тип параметра 'TopicsOrPattern'.";
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("consumer/subscribe", ConsumerSubscribeRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerGetSubscription(Consumer) Экспорт
		
	ConsumerGetSubscriptionRequest = Новый Структура;
	ConsumerGetSubscriptionRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetSubscriptionRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/get-subscription", ConsumerGetSubscriptionRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerUnsubscribe(Consumer) Экспорт
	
	ConsumerUnsubscribeRequest = Новый Структура;
	ConsumerUnsubscribeRequest.Вставить("consumerId", Consumer.id);
	ConsumerUnsubscribeRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/unsubscribe", ConsumerUnsubscribeRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerSeek(Consumer, Topic, Partition, Offset) Экспорт
	
	ConsumerSeekRequest = Новый Структура;
	ConsumerSeekRequest.Вставить("consumerId", Consumer.id);
	ConsumerSeekRequest.Вставить("token", Consumer.token);
	ConsumerSeekRequest.Вставить("topic", Topic);
	ConsumerSeekRequest.Вставить("partition", Partition);
	ConsumerSeekRequest.Вставить("offset", Offset);
	
	HttpОтвет = ВыполнитьPost("consumer/seek", ConsumerSeekRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerSeekToBeginning(Consumer, Partitions) Экспорт
	
	ConsumerSeekToRequest = Новый Структура;
	ConsumerSeekToRequest.Вставить("consumerId", Consumer.id);
	ConsumerSeekToRequest.Вставить("token", Consumer.token);
	ConsumerSeekToRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("consumer/seek-to-beginning", ConsumerSeekToRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerSeekToEnd(Consumer, Partitions) Экспорт
	
	ConsumerSeekToRequest = Новый Структура;
	ConsumerSeekToRequest.Вставить("consumerId", Consumer.id);
	ConsumerSeekToRequest.Вставить("token", Consumer.token);
	ConsumerSeekToRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("consumer/seek-to-end", ConsumerSeekToRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerGetPosition(Consumer, Topic, Partition) Экспорт
		
	ConsumerGetPositionRequest = Новый Структура;
	ConsumerGetPositionRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetPositionRequest.Вставить("token", Consumer.token);
	ConsumerGetPositionRequest.Вставить("topic", Topic);
	ConsumerGetPositionRequest.Вставить("partition", Partition);
	
	HttpОтвет = ВыполнитьPost("consumer/get-position", ConsumerGetPositionRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerGetBeginningOffsets(Consumer, Partitions) Экспорт
		
	ConsumerGetOffsetsRequest = Новый Структура;
	ConsumerGetOffsetsRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetOffsetsRequest.Вставить("token", Consumer.token);
	ConsumerGetOffsetsRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("consumer/get-beginning-offsets", ConsumerGetOffsetsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerGetEndOffsets(Consumer, Partitions) Экспорт
		
	ConsumerGetOffsetsRequest = Новый Структура;
	ConsumerGetOffsetsRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetOffsetsRequest.Вставить("token", Consumer.token);
	ConsumerGetOffsetsRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("consumer/get-end-offsets", ConsumerGetOffsetsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerPoll(Consumer, Timeout) Экспорт
		
	HttpОтвет = ConsumerPollRaw(Consumer, Timeout);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerPollRaw(Consumer, Timeout, Accept = Неопределено) Экспорт
		
	ConsumerPollRequest = Новый Структура;
	ConsumerPollRequest.Вставить("consumerId", Consumer.id);
	ConsumerPollRequest.Вставить("token", Consumer.token);
	ConsumerPollRequest.Вставить("timeout", Timeout);
	
	Возврат ВыполнитьPost("consumer/poll", ConsumerPollRequest, Accept);
	
КонецФункции

Функция ConsumerCommit(Consumer) Экспорт
		
	ConsumerCommitRequest = Новый Структура;
	ConsumerCommitRequest.Вставить("consumerId", Consumer.id);
	ConsumerCommitRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/commit", ConsumerCommitRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция ConsumerGetCommitted(Consumer, Partitions) Экспорт
		
	ConsumerGetCommittedRequest = Новый Структура;
	ConsumerGetCommittedRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetCommittedRequest.Вставить("token", Consumer.token);
	ConsumerGetCommittedRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("consumer/get-committed", ConsumerGetCommittedRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция ConsumerGetGroupMetadata(Consumer) Экспорт
		
	ConsumerGetGroupMetadataRequest = Новый Структура;
	ConsumerGetGroupMetadataRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetGroupMetadataRequest.Вставить("token", Consumer.token);
	
	HttpОтвет = ВыполнитьPost("consumer/get-group-metadata", ConsumerGetGroupMetadataRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

#КонецОбласти

#Область Администрирование

Функция AdminCreate(Name, Config, Знач ExpirationTimeout = Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	AdminCreateRequest = Новый Структура;
	AdminCreateRequest.Вставить("name", Name);
	AdminCreateRequest.Вставить("config", Config);
	AdminCreateRequest.Вставить("expirationTimeout", ExpirationTimeout);
	
	HttpОтвет = ВыполнитьPost("admin/create", AdminCreateRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminRelease(Admin) Экспорт
			
	AdminReleaseRequest = Новый Структура;
	AdminReleaseRequest.Вставить("adminId", Admin.id);
	AdminReleaseRequest.Вставить("token", Admin.token);
	
	HttpОтвет = ВыполнитьPost("admin/release", AdminReleaseRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminList() Экспорт
	
	HttpОтвет = ВыполнитьGet("admin");
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminTouch(Admin) Экспорт
	
	AdminTouchRequest = Новый Структура;
	AdminTouchRequest.Вставить("adminId", Admin.id);
	AdminTouchRequest.Вставить("token", Admin.token);
	
	HttpОтвет = ВыполнитьPost("admin/touch", AdminTouchRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDescribeCluster(Admin) Экспорт
		
	AdminDescribeClusterRequest = Новый Структура;
	AdminDescribeClusterRequest.Вставить("adminId", Admin.id);
	AdminDescribeClusterRequest.Вставить("token", Admin.token);
	
	HttpОтвет = ВыполнитьPost("admin/describe-cluster", AdminDescribeClusterRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeLogDirs(Admin, BrokerIds) Экспорт
		
	AdminDescribeLogDirsRequest = Новый Структура;
	AdminDescribeLogDirsRequest.Вставить("adminId", Admin.id);
	AdminDescribeLogDirsRequest.Вставить("token", Admin.token);
	AdminDescribeLogDirsRequest.Вставить("brokerIds", BrokerIds);
	
	HttpОтвет = ВыполнитьPost("admin/describe-log-dirs", AdminDescribeLogDirsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListTopics(Admin, IncludeInternal = Неопределено, Pattern = Неопределено) Экспорт
		
	AdminListTopicsRequest = Новый Структура;
	AdminListTopicsRequest.Вставить("adminId", Admin.id);
	AdminListTopicsRequest.Вставить("token", Admin.token);
	Если IncludeInternal <> Неопределено Тогда
		AdminListTopicsRequest.Вставить("includeInternal", IncludeInternal);
	КонецЕсли;
	Если Pattern <> Неопределено Тогда
		AdminListTopicsRequest.Вставить("pattern", Pattern);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-topics", AdminListTopicsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminCreateTopic(Admin, TopicName, NumPartitions = Неопределено, ReplicationFactor = Неопределено) Экспорт
	
	AdminCreateTopicRequest = Новый Структура;
	AdminCreateTopicRequest.Вставить("adminId", Admin.id);
	AdminCreateTopicRequest.Вставить("token", Admin.token);
	AdminCreateTopicRequest.Вставить("topicName", TopicName);
	Если NumPartitions <> Неопределено Тогда
		AdminCreateTopicRequest.Вставить("numPartitions", NumPartitions);
	КонецЕсли;
	Если ReplicationFactor <> Неопределено Тогда
		AdminCreateTopicRequest.Вставить("replicationFactor", ReplicationFactor);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/create-topic", AdminCreateTopicRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminCreateTopics(Admin, Topics) Экспорт
	
	AdminCreateTopicsRequest = Новый Структура;
	AdminCreateTopicsRequest.Вставить("adminId", Admin.id);
	AdminCreateTopicsRequest.Вставить("token", Admin.token);
	AdminCreateTopicsRequest.Вставить("topics", Topics);
	
	HttpОтвет = ВыполнитьPost("admin/create-topics", AdminCreateTopicsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDeleteTopic(Admin, TopicName) Экспорт
	
	AdminDeleteTopicRequest = Новый Структура;
	AdminDeleteTopicRequest.Вставить("adminId", Admin.id);
	AdminDeleteTopicRequest.Вставить("token", Admin.token);
	AdminDeleteTopicRequest.Вставить("topicName", TopicName);
	
	HttpОтвет = ВыполнитьPost("admin/delete-topic", AdminDeleteTopicRequest);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteTopics(Admin, TopicNames) Экспорт
	
	AdminDeleteTopicsRequest = Новый Структура;
	AdminDeleteTopicsRequest.Вставить("adminId", Admin.id);
	AdminDeleteTopicsRequest.Вставить("token", Admin.token);
	AdminDeleteTopicsRequest.Вставить("topicNames", TopicNames);
	
	HttpОтвет = ВыполнитьPost("admin/delete-topics", AdminDeleteTopicsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDeleteRecords(Admin, Topic, Partition, BeforeOffset) Экспорт
	
	AdminDeleteRecordsRequest = Новый Структура;
	AdminDeleteRecordsRequest.Вставить("adminId", Admin.id);
	AdminDeleteRecordsRequest.Вставить("token", Admin.token);
	AdminDeleteRecordsRequest.Вставить("topic", Topic);
	AdminDeleteRecordsRequest.Вставить("partition", Partition);
	AdminDeleteRecordsRequest.Вставить("beforeOffset", BeforeOffset);
	
	HttpОтвет = ВыполнитьPost("admin/delete-records", AdminDeleteRecordsRequest);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeTopic(Admin, TopicName, IncludeAuthorizedOperations = Неопределено) Экспорт
		
	AdminDescribeTopicRequest = Новый Структура;
	AdminDescribeTopicRequest.Вставить("adminId", Admin.id);
	AdminDescribeTopicRequest.Вставить("token", Admin.token);
	AdminDescribeTopicRequest.Вставить("topicName", TopicName);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeTopicRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-topic", AdminDescribeTopicRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminCreatePartitions(Admin, Topic, IncreaseTo) Экспорт
		
	AdminCreatePartitionsRequest = Новый Структура;
	AdminCreatePartitionsRequest.Вставить("adminId", Admin.id);
	AdminCreatePartitionsRequest.Вставить("token", Admin.token);
	AdminCreatePartitionsRequest.Вставить("topicName", Topic);
	AdminCreatePartitionsRequest.Вставить("increaseTo", IncreaseTo);
	
	HttpОтвет = ВыполнитьPost("admin/create-partitions", AdminCreatePartitionsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDescribeBrokerConfigs(Admin, BrokerId) Экспорт
		
	AdminDescribeBrokerConfigsRequest = Новый Структура;
	AdminDescribeBrokerConfigsRequest.Вставить("adminId", Admin.id);
	AdminDescribeBrokerConfigsRequest.Вставить("token", Admin.token);
	AdminDescribeBrokerConfigsRequest.Вставить("brokerId", BrokerId);
	
	HttpОтвет = ВыполнитьPost("admin/describe-broker-configs", AdminDescribeBrokerConfigsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeTopicConfigs(Admin, TopicName) Экспорт
	
	AdminDescribeTopicConfigsRequest = Новый Структура;
	AdminDescribeTopicConfigsRequest.Вставить("adminId", Admin.id);
	AdminDescribeTopicConfigsRequest.Вставить("token", Admin.token);
	AdminDescribeTopicConfigsRequest.Вставить("topicName", TopicName);
	
	HttpОтвет = ВыполнитьPost("admin/describe-topic-configs", AdminDescribeTopicConfigsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeGroupConfigs(Admin, GroupId) Экспорт
	
	AdminDescribeGroupConfigsRequest = Новый Структура;
	AdminDescribeGroupConfigsRequest.Вставить("adminId", Admin.id);
	AdminDescribeGroupConfigsRequest.Вставить("token", Admin.token);
	AdminDescribeGroupConfigsRequest.Вставить("groupId", GroupId);
	
	HttpОтвет = ВыполнитьPost("admin/describe-group-configs", AdminDescribeGroupConfigsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminAlterTopicConfig(Admin, TopicName, ConfigName, NewValue) Экспорт
		
	AdminAlterTopicConfigRequest = Новый Структура;
	AdminAlterTopicConfigRequest.Вставить("adminId", Admin.id);
	AdminAlterTopicConfigRequest.Вставить("token", Admin.token);
	AdminAlterTopicConfigRequest.Вставить("topicName", TopicName);
	AdminAlterTopicConfigRequest.Вставить("configName", ConfigName);
	AdminAlterTopicConfigRequest.Вставить("newValue", NewValue);
	
	HttpОтвет = ВыполнитьPost("admin/alter-topic-config", AdminAlterTopicConfigRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminAlterGroupConfig(Admin, GroupId, ConfigName, NewValue) Экспорт
		
	AdminAlterGroupConfigRequest = Новый Структура;
	AdminAlterGroupConfigRequest.Вставить("adminId", Admin.id);
	AdminAlterGroupConfigRequest.Вставить("token", Admin.token);
	AdminAlterGroupConfigRequest.Вставить("groupId", GroupId);
	AdminAlterGroupConfigRequest.Вставить("configName", ConfigName);
	AdminAlterGroupConfigRequest.Вставить("newValue", NewValue);
	
	HttpОтвет = ВыполнитьPost("admin/alter-group-config", AdminAlterGroupConfigRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteTopicConfig(Admin, TopicName, ConfigName) Экспорт
		
	AdminDeleteTopicConfigRequest = Новый Структура;
	AdminDeleteTopicConfigRequest.Вставить("adminId", Admin.id);
	AdminDeleteTopicConfigRequest.Вставить("token", Admin.token);
	AdminDeleteTopicConfigRequest.Вставить("topicName", TopicName);
	AdminDeleteTopicConfigRequest.Вставить("configName", ConfigName);
	
	HttpОтвет = ВыполнитьPost("admin/delete-topic-config", AdminDeleteTopicConfigRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteGroupConfig(Admin, GroupId, ConfigName) Экспорт
		
	AdminDeleteGroupConfigRequest = Новый Структура;
	AdminDeleteGroupConfigRequest.Вставить("adminId", Admin.id);
	AdminDeleteGroupConfigRequest.Вставить("token", Admin.token);
	AdminDeleteGroupConfigRequest.Вставить("groupId", GroupId);
	AdminDeleteGroupConfigRequest.Вставить("configName", ConfigName);
	
	HttpОтвет = ВыполнитьPost("admin/delete-group-config", AdminDeleteGroupConfigRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDescribeUserScramCredentials(Admin, Users = Неопределено) Экспорт
	
	AdminDescribeUserScramCredentialsRequest = Новый Структура;
	AdminDescribeUserScramCredentialsRequest.Вставить("adminId", Admin.id);
	AdminDescribeUserScramCredentialsRequest.Вставить("token", Admin.token);
	Если Users <> Неопределено Тогда
		AdminDescribeUserScramCredentialsRequest.Вставить("users", Users);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-user-scram-credentials", AdminDescribeUserScramCredentialsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminUpsertUserScramCredentials(Admin, User, Mechanism, Password, Iterations = Неопределено) Экспорт
	
	AdminUpsertUserScramCredentialsRequest = Новый Структура;
	AdminUpsertUserScramCredentialsRequest.Вставить("adminId", Admin.id);
	AdminUpsertUserScramCredentialsRequest.Вставить("token", Admin.token);
	AdminUpsertUserScramCredentialsRequest.Вставить("user", User);
	AdminUpsertUserScramCredentialsRequest.Вставить("mechanism", Mechanism);
	AdminUpsertUserScramCredentialsRequest.Вставить("password", Password);
	Если Iterations <> Неопределено Тогда
		AdminUpsertUserScramCredentialsRequest.Вставить("iterations", Iterations);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/upsert-user-scram-credentials", AdminUpsertUserScramCredentialsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteUserScramCredentials(Admin, User, Mechanism) Экспорт
	
	AdminDeleteUserScramCredentialsRequest = Новый Структура;
	AdminDeleteUserScramCredentialsRequest.Вставить("adminId", Admin.id);
	AdminDeleteUserScramCredentialsRequest.Вставить("token", Admin.token);
	AdminDeleteUserScramCredentialsRequest.Вставить("user", User);
	AdminDeleteUserScramCredentialsRequest.Вставить("mechanism", Mechanism);
	
	HttpОтвет = ВыполнитьPost("admin/delete-user-scram-credentials", AdminDeleteUserScramCredentialsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDescribeAcls(Admin, AclBindingFilter = Неопределено) Экспорт
	
	AdminDescribeAclsRequest = Новый Структура;
	AdminDescribeAclsRequest.Вставить("adminId", Admin.id);
	AdminDescribeAclsRequest.Вставить("token", Admin.token);
	Если AclBindingFilter <> Неопределено Тогда
		AdminDescribeAclsRequest.Вставить("filter", AclBindingFilter);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-acls", AdminDescribeAclsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminCreateAcls(Admin, AclBindings) Экспорт
	
	AdminCreateAclsRequest = Новый Структура;
	AdminCreateAclsRequest.Вставить("adminId", Admin.id);
	AdminCreateAclsRequest.Вставить("token", Admin.token);
	AdminCreateAclsRequest.Вставить("acls", AclBindings);
	
	HttpОтвет = ВыполнитьPost("admin/create-acls", AdminCreateAclsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteAcls(Admin, AclBindingFilters) Экспорт
	
	AdminDeleteAclsRequest = Новый Структура;
	AdminDeleteAclsRequest.Вставить("adminId", Admin.id);
	AdminDeleteAclsRequest.Вставить("token", Admin.token);
	AdminDeleteAclsRequest.Вставить("filters", AclBindingFilters);
	
	HttpОтвет = ВыполнитьPost("admin/delete-acls", AdminDeleteAclsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDescribeProducers(Admin, Partitions) Экспорт
	
	AdminDescribeProducersRequest = Новый Структура;
	AdminDescribeProducersRequest.Вставить("adminId", Admin.id);
	AdminDescribeProducersRequest.Вставить("token", Admin.token);
	AdminDescribeProducersRequest.Вставить("partitions", Partitions);
	
	HttpОтвет = ВыполнитьPost("admin/describe-producers", AdminDescribeProducersRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminAbortTransaction(Admin, TopicPartition, ProducerId, ProducerEpoch, CoordinatorEpoch) Экспорт
	
	AdminAbortTransactionRequest = Новый Структура;
	AdminAbortTransactionRequest.Вставить("adminId", Admin.id);
	AdminAbortTransactionRequest.Вставить("token", Admin.token);
	AdminAbortTransactionRequest.Вставить("partition", TopicPartition);
	AdminAbortTransactionRequest.Вставить("producerId", ProducerId);
	AdminAbortTransactionRequest.Вставить("producerEpoch", ProducerEpoch);
	AdminAbortTransactionRequest.Вставить("coordinatorEpoch", CoordinatorEpoch);
	
	HttpОтвет = ВыполнитьPost("admin/abort-transaction", AdminAbortTransactionRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminListGroups(Admin, WithTypes = Неопределено, WithProtocolTypes = Неопределено, InStates = Неопределено) Экспорт
	
	AdminListGroupsRequest = Новый Структура;
	AdminListGroupsRequest.Вставить("adminId", Admin.id);
	AdminListGroupsRequest.Вставить("token", Admin.token);
	Если WithTypes <> Неопределено Тогда
		AdminListGroupsRequest.Вставить("withTypes", WithTypes);
	КонецЕсли;
	Если WithProtocolTypes <> Неопределено Тогда
		AdminListGroupsRequest.Вставить("withProtocolTypes", WithProtocolTypes);
	КонецЕсли;
	Если InStates <> Неопределено Тогда
		AdminListGroupsRequest.Вставить("inStates", InStates);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-groups", AdminListGroupsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeClassicGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	AdminDescribeClassicGroupRequest = Новый Структура;
	AdminDescribeClassicGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeClassicGroupRequest.Вставить("token", Admin.token);
	AdminDescribeClassicGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeClassicGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-classic-group", AdminDescribeClassicGroupRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeConsumerGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	AdminDescribeConsumerGroupRequest = Новый Структура;
	AdminDescribeConsumerGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeConsumerGroupRequest.Вставить("token", Admin.token);
	AdminDescribeConsumerGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeConsumerGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-consumer-group", AdminDescribeConsumerGroupRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeShareGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	AdminDescribeShareGroupRequest = Новый Структура;
	AdminDescribeShareGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeShareGroupRequest.Вставить("token", Admin.token);
	AdminDescribeShareGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeShareGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-share-group", AdminDescribeShareGroupRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeStreamsGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	AdminDescribeStreamsGroupRequest = Новый Структура;
	AdminDescribeStreamsGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeStreamsGroupRequest.Вставить("token", Admin.token);
	AdminDescribeStreamsGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeStreamsGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/describe-streams-group", AdminDescribeStreamsGroupRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListConsumerGroupOffsets(Admin, GroupId, RequireStable = Неопределено) Экспорт
	
	AdminListConsumerGroupOffsetsRequest = Новый Структура;
	AdminListConsumerGroupOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListConsumerGroupOffsetsRequest.Вставить("token", Admin.token);
	AdminListConsumerGroupOffsetsRequest.Вставить("groupId", GroupId);
	Если RequireStable <> Неопределено Тогда
		AdminListConsumerGroupOffsetsRequest.Вставить("requireStable", RequireStable);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-consumer-group-offsets", AdminListConsumerGroupOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminAlterConsumerGroupOffsets(Admin, GroupId, Offsets) Экспорт
	
	AdminAlterConsumerGroupOffsetsRequest = Новый Структура;
	AdminAlterConsumerGroupOffsetsRequest.Вставить("adminId", Admin.id);
	AdminAlterConsumerGroupOffsetsRequest.Вставить("token", Admin.token);
	AdminAlterConsumerGroupOffsetsRequest.Вставить("groupId", GroupId);
	AdminAlterConsumerGroupOffsetsRequest.Вставить("offsets", Новый Массив);
	Для Каждого OffsetsItem Из Offsets Цикл
		TopicPartitionOffsetMetadata = Новый Структура;
		TopicPartitionOffsetMetadata.Вставить("topic", OffsetsItem.Topic);
		TopicPartitionOffsetMetadata.Вставить("partition", OffsetsItem.Partition);
		TopicPartitionOffsetMetadata.Вставить("offset", OffsetsItem.Offset);
		Если OffsetsItem.Свойство("Metadata") Тогда
			TopicPartitionOffsetMetadata.Вставить("metadata", OffsetsItem.Metadata);
		КонецЕсли;
		AdminAlterConsumerGroupOffsetsRequest.offsets.Добавить(TopicPartitionOffsetMetadata);
	КонецЦикла;
	
	HttpОтвет = ВыполнитьPost("admin/alter-consumer-group-offsets", AdminAlterConsumerGroupOffsetsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteConsumerGroupOffsets(Admin, GroupId, Partitions) Экспорт
	
	AdminDeleteConsumerGroupOffsetsRequest = Новый Структура;
	AdminDeleteConsumerGroupOffsetsRequest.Вставить("adminId", Admin.id);
	AdminDeleteConsumerGroupOffsetsRequest.Вставить("token", Admin.token);
	AdminDeleteConsumerGroupOffsetsRequest.Вставить("groupId", GroupId);
	AdminDeleteConsumerGroupOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminDeleteConsumerGroupOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	
	HttpОтвет = ВыполнитьPost("admin/delete-consumer-group-offsets", AdminDeleteConsumerGroupOffsetsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminRemoveMembersFromConsumerGroup(Admin, GroupId, Members = Неопределено, Reason = Неопределено) Экспорт
	
	AdminRemoveMembersFromConsumerGroupRequest = Новый Структура;
	AdminRemoveMembersFromConsumerGroupRequest.Вставить("adminId", Admin.id);
	AdminRemoveMembersFromConsumerGroupRequest.Вставить("token", Admin.token);
	AdminRemoveMembersFromConsumerGroupRequest.Вставить("groupId", GroupId);
	Если Members <> Неопределено Тогда
		AdminRemoveMembersFromConsumerGroupRequest.Вставить("members", Members);
	КонецЕсли;
	Если Reason <> Неопределено Тогда
		AdminRemoveMembersFromConsumerGroupRequest.Вставить("reason", Reason);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/remove-members-from-consumer-group", AdminRemoveMembersFromConsumerGroupRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteConsumerGroup(Admin, GroupId) Экспорт
	
	AdminDeleteConsumerGroupRequest = Новый Структура;
	AdminDeleteConsumerGroupRequest.Вставить("adminId", Admin.id);
	AdminDeleteConsumerGroupRequest.Вставить("token", Admin.token);
	AdminDeleteConsumerGroupRequest.Вставить("groupId", GroupId);
	
	HttpОтвет = ВыполнитьPost("admin/delete-consumer-group", AdminDeleteConsumerGroupRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteConsumerGroups(Admin, GroupIds) Экспорт
	
	AdminDeleteConsumerGroupsRequest = Новый Структура;
	AdminDeleteConsumerGroupsRequest.Вставить("adminId", Admin.id);
	AdminDeleteConsumerGroupsRequest.Вставить("token", Admin.token);
	AdminDeleteConsumerGroupsRequest.Вставить("groupIds", GroupIds);
	
	HttpОтвет = ВыполнитьPost("admin/delete-consumer-groups", AdminDeleteConsumerGroupsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteShareGroup(Admin, GroupId) Экспорт
	
	AdminDeleteShareGroupRequest = Новый Структура;
	AdminDeleteShareGroupRequest.Вставить("adminId", Admin.id);
	AdminDeleteShareGroupRequest.Вставить("token", Admin.token);
	AdminDeleteShareGroupRequest.Вставить("groupId", GroupId);
	
	HttpОтвет = ВыполнитьPost("admin/delete-share-group", AdminDeleteShareGroupRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteShareGroups(Admin, GroupIds) Экспорт
	
	AdminDeleteShareGroupsRequest = Новый Структура;
	AdminDeleteShareGroupsRequest.Вставить("adminId", Admin.id);
	AdminDeleteShareGroupsRequest.Вставить("token", Admin.token);
	AdminDeleteShareGroupsRequest.Вставить("groupIds", GroupIds);
	
	HttpОтвет = ВыполнитьPost("admin/delete-share-groups", AdminDeleteShareGroupsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteStreamsGroup(Admin, GroupId) Экспорт
	
	AdminDeleteStreamsGroupRequest = Новый Структура;
	AdminDeleteStreamsGroupRequest.Вставить("adminId", Admin.id);
	AdminDeleteStreamsGroupRequest.Вставить("token", Admin.token);
	AdminDeleteStreamsGroupRequest.Вставить("groupId", GroupId);
	
	HttpОтвет = ВыполнитьPost("admin/delete-streams-group", AdminDeleteStreamsGroupRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminDeleteStreamsGroups(Admin, GroupIds) Экспорт
	
	AdminDeleteStreamsGroupsRequest = Новый Структура;
	AdminDeleteStreamsGroupsRequest.Вставить("adminId", Admin.id);
	AdminDeleteStreamsGroupsRequest.Вставить("token", Admin.token);
	AdminDeleteStreamsGroupsRequest.Вставить("groupIds", GroupIds);
	
	HttpОтвет = ВыполнитьPost("admin/delete-streams-groups", AdminDeleteStreamsGroupsRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

Функция AdminListEarliestOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	AdminListOffsetsRequest = Новый Структура;
	AdminListOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListOffsetsRequest.Вставить("token", Admin.token);
	AdminListOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminListOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	Если IsolationLevel <> Неопределено Тогда
		AdminListOffsetsRequest.Вставить("isolationLevel", IsolationLevel);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-earliest-offsets", AdminListOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListEarliestLocalOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	AdminListOffsetsRequest = Новый Структура;
	AdminListOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListOffsetsRequest.Вставить("token", Admin.token);
	AdminListOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminListOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	Если IsolationLevel <> Неопределено Тогда
		AdminListOffsetsRequest.Вставить("isolationLevel", IsolationLevel);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-earliest-local-offsets", AdminListOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListLatestOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	AdminListOffsetsRequest = Новый Структура;
	AdminListOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListOffsetsRequest.Вставить("token", Admin.token);
	AdminListOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminListOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	Если IsolationLevel <> Неопределено Тогда
		AdminListOffsetsRequest.Вставить("isolationLevel", IsolationLevel);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-latest-offsets", AdminListOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListLatestTieredOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	AdminListOffsetsRequest = Новый Структура;
	AdminListOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListOffsetsRequest.Вставить("token", Admin.token);
	AdminListOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminListOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	Если IsolationLevel <> Неопределено Тогда
		AdminListOffsetsRequest.Вставить("isolationLevel", IsolationLevel);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-latest-tiered-offsets", AdminListOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListMaxTimestampOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	AdminListOffsetsRequest = Новый Структура;
	AdminListOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListOffsetsRequest.Вставить("token", Admin.token);
	AdminListOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminListOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	Если IsolationLevel <> Неопределено Тогда
		AdminListOffsetsRequest.Вставить("isolationLevel", IsolationLevel);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-max-timestamp-offsets", AdminListOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminListTimestampOffsets(Admin, Partitions, Timestamp, IsolationLevel = Неопределено) Экспорт
	
	AdminListOffsetsRequest = Новый Структура;
	AdminListOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListOffsetsRequest.Вставить("token", Admin.token);
	AdminListOffsetsRequest.Вставить("partitions", Новый Массив);
	Для Каждого PartitionsItem Из Partitions Цикл
		TopicPartition = Новый Структура;
		TopicPartition.Вставить("topic", PartitionsItem.Topic);
		TopicPartition.Вставить("partition", PartitionsItem.Partition);
		AdminListOffsetsRequest.partitions.Добавить(TopicPartition);
	КонецЦикла;
	AdminListOffsetsRequest.Вставить("timestamp", Timestamp);
	Если IsolationLevel <> Неопределено Тогда
		AdminListOffsetsRequest.Вставить("isolationLevel", IsolationLevel);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/list-timestamp-offsets", AdminListOffsetsRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminDescribeFeatures(Admin) Экспорт
	
	AdminDescribeFeaturesRequest = Новый Структура;
	AdminDescribeFeaturesRequest.Вставить("adminId", Admin.id);
	AdminDescribeFeaturesRequest.Вставить("token", Admin.token);
	
	HttpОтвет = ВыполнитьPost("admin/describe-features", AdminDescribeFeaturesRequest);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

Функция AdminUpdateFeature(Admin, FeatureName, VersionLevel, UpgradeType, ValidateOnly = Неопределено) Экспорт
	
	AdminUpdateFeatureRequest = Новый Структура;
	AdminUpdateFeatureRequest.Вставить("adminId", Admin.id);
	AdminUpdateFeatureRequest.Вставить("token", Admin.token);
	AdminUpdateFeatureRequest.Вставить("featureName", FeatureName);
	AdminUpdateFeatureRequest.Вставить("versionLevel", VersionLevel);
	AdminUpdateFeatureRequest.Вставить("upgradeType", UpgradeType);
	Если ValidateOnly = Неопределено Тогда
		AdminUpdateFeatureRequest.Вставить("validateOnly", ValidateOnly);
	КонецЕсли;
	
	HttpОтвет = ВыполнитьPost("admin/update-feature", AdminUpdateFeatureRequest);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции

#КонецОбласти

Функция GetVersion() Экспорт
	
	HttpОтвет = ВыполнитьGet("version");
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции

#Область ВспомогательныеФункции

Функция ПолучитьHttpСоединение() Экспорт
	
	Возврат HttpСоединение;
	
КонецФункции

Процедура ЗаписатьJsonВHttpЗапрос(HttpЗапрос, Значение)
	
	HttpЗапрос.Заголовки.Вставить("Content-Type", "application/json; charset=utf-8");
	
	Поток = HttpЗапрос.ПолучитьТелоКакПоток();
	
	ЗаписьJson = Новый ЗаписьJson;
	ЗаписьJson.ОткрытьПоток(Поток, "UTF-8", Ложь, ПараметрыЗаписиJson);
	ЗаписатьJson(ЗаписьJson, Значение);
	ЗаписьJson.Закрыть();
	
	Поток.Закрыть();
	
КонецПроцедуры

Функция ВыполнитьGet(Путь, Accept = Неопределено)

	HttpЗапрос = Новый HttpЗапрос(Путь);

	Если Accept <> Неопределено Тогда
		HttpЗапрос.Заголовки.Вставить("Accept", Accept);
    КонецЕсли;

	HttpОтвет = HttpСоединение.Получить(HttpЗапрос);

	Возврат ПроверитьHttpОтвет(HttpОтвет);

КонецФункции

Функция ВыполнитьPost(Путь, Тело, Accept = Неопределено)
	
	HttpЗапрос = Новый HttpЗапрос(Путь);
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, Тело);
	
	Если Accept <> Неопределено Тогда
		HttpЗапрос.Заголовки.Вставить("Accept", Accept);
	КонецЕсли;
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ПроверитьHttpОтвет(HttpОтвет) Экспорт
	
	КодОтвета = HttpОтвет.КодСостояния;
	
	Если HttpОтвет.КодСостояния < 200 Или HttpОтвет.КодСостояния > 299 Тогда
		ОписаниеОшибки = HttpОтвет.ПолучитьТелоКакСтроку();
		Возврат Неопределено;
	КонецЕсли;
	
	ОписаниеОшибки = Неопределено;
	Возврат HttpОтвет;
	
КонецФункции

Функция ПрочитатьТелоHttpОтвета(HttpОтвет)
	
	Если HttpОтвет = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
	
	Если HttpОтвет.КодСостояния = 204 Тогда
		Возврат Null;
	КонецЕсли;
		
	ContentType = HttpОтвет.Заголовки.Получить("Content-Type");
	Если ContentType = Неопределено Тогда
		
		ContentLength = HttpОтвет.Заголовки.Получить("Content-Length");
		Если ContentLength = "0" Тогда
			Возврат Null;
		Иначе
			ВызватьИсключение "Неожиданное состояние.";
		КонецЕсли;
		
	ИначеЕсли СтрНачинаетсяС(ContentType, "application/json") Тогда
		
		Поток = HttpОтвет.ПолучитьТелоКакПоток();
		
		ЧтениеJson = Новый ЧтениеJson;
		ЧтениеJson.ОткрытьПоток(Поток);
		Результат = ПрочитатьJson(ЧтениеJson);
		ЧтениеJson.Закрыть();
		
		Поток.Закрыть();
		
	ИначеЕсли СтрНачинаетсяС(ContentType, "text/plain") Тогда
		
		Результат = HttpОтвет.ПолучитьТелоКакСтроку();
		
	ИначеЕсли СтрНачинаетсяС(ContentType, "application/octet-stream") Тогда
		
		Результат = HttpОтвет.ПолучитьТелоКакДвоичныеДанные();
		
	Иначе
		
		ВызватьИсключение "Неожиданный формат возвращенных данных: '"+ContentType+"'.";
		
	КонецЕсли;
		
	Возврат Результат;
	
КонецФункции

#КонецОбласти
