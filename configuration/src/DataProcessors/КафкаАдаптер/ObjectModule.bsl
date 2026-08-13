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
// Версия 5.1.
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
	
	HttpОтвет = ProducerCreate_(Name, Config, ExpirationTimeout);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);;
	
КонецФункции
Функция ProducerCreate_(Name, Config, ExpirationTimeout)
		
	ProducerCreateRequest = Новый Структура;
	ProducerCreateRequest.Вставить("name", Name);
	ProducerCreateRequest.Вставить("config", Config);
	ProducerCreateRequest.Вставить("expirationTimeout", ExpirationTimeout);
	
	HttpЗапрос = Новый HttpЗапрос("producer/create");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerCreateRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerRelease(Producer) Экспорт
	
	HttpОтвет = ProducerRelease_(Producer);
	
	Возврат (HttpОтвет <> Неопределено);
	
КонецФункции
Функция ProducerRelease_(Producer)
		
	ProducerReleaseRequest = Новый Структура;
	ProducerReleaseRequest.Вставить("producerId", Producer.id);
	ProducerReleaseRequest.Вставить("token", Producer.token);
	
	HttpЗапрос = Новый HTTPЗапрос("producer/release");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerReleaseRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции	

Функция ProducerList() Экспорт
		
	HttpОтвет = ProducerList_();
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ProducerList_()
	
	HttpЗапрос = Новый HttpЗапрос("producer");
	
	HttpОтвет = HttpСоединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerTouch(Producer) Экспорт
	
	HttpОтвет = ProducerTouch_(Producer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ProducerTouch_(Producer)
	
	ProducerTouchRequest = Новый Структура;
	ProducerTouchRequest.Вставить("producerId", Producer.id);
	ProducerTouchRequest.Вставить("token", Producer.token);
	
	HttpЗапрос = Новый HttpЗапрос("producer/touch");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerTouchRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerGetPartitions(Producer, Topic) Экспорт
	
	HttpОтвет = ProducerGetPartitions_(Producer, Topic);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ProducerGetPartitions_(Producer, Topic)
		
	ProducerGetPartitionsRequest = Новый Структура;
	ProducerGetPartitionsRequest.Вставить("producerId", Producer.id);
	ProducerGetPartitionsRequest.Вставить("token", Producer.token);
	ProducerGetPartitionsRequest.Вставить("topic", Topic);
	
	HttpЗапрос = Новый HTTPЗапрос("producer/get-partitions");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerGetPartitionsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции	

Функция ProducerSend(Producer, Topic, KeyString = Неопределено, ValueString, Headers = Неопределено) Экспорт
	
	HttpОтвет = ProducerSend_(Producer, Topic, KeyString, ValueString, Headers);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ProducerSend_(Producer, Topic, KeyString, ValueString, Headers)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("producer/send");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerSendStringRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerBeginTransaction(Producer) Экспорт
	
	HttpОтвет = ProducerBeginTransaction_(Producer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ProducerBeginTransaction_(Producer)
	
	ProducerBeginTransactionRequest = Новый Структура;
	ProducerBeginTransactionRequest.Вставить("producerId", Producer.id);
	ProducerBeginTransactionRequest.Вставить("token", Producer.token);
	
	HttpЗапрос = Новый HttpЗапрос("producer/begin-transaction");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerBeginTransactionRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerCommitTransaction(Producer) Экспорт
	
	HttpОтвет = ProducerCommitTransaction_(Producer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ProducerCommitTransaction_(Producer)
	
	ProducerCommitTransactionRequest = Новый Структура;
	ProducerCommitTransactionRequest.Вставить("producerId", Producer.id);
	ProducerCommitTransactionRequest.Вставить("token", Producer.token);
	
	HttpЗапрос = Новый HttpЗапрос("producer/commit-transaction");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerCommitTransactionRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerAbortTransaction(Producer) Экспорт
	
	HttpОтвет = ProducerAbortTransaction_(Producer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ProducerAbortTransaction_(Producer)
	
	ProducerAbortTransactionRequest = Новый Структура;
	ProducerAbortTransactionRequest.Вставить("producerId", Producer.id);
	ProducerAbortTransactionRequest.Вставить("token", Producer.token);
	
	HttpЗапрос = Новый HttpЗапрос("producer/abort-transaction");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ProducerAbortTransactionRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

#КонецОбласти

#Область Получение

Функция ConsumerCreate(Name, Config, Знач ExpirationTimeout = Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	HttpОтвет = ConsumerCreate_(Name, Config, ExpirationTimeout);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerCreate_(Name, Config, ExpirationTimeout)
		
	ConsumerCreateRequest = Новый Структура;
	ConsumerCreateRequest.Вставить("name", Name);
	ConsumerCreateRequest.Вставить("config", Config);
	ConsumerCreateRequest.Вставить("expirationTimeout", ExpirationTimeout);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/create");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerCreateRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerRelease(Consumer) Экспорт
			
	HttpОтвет = ConsumerRelease_(Consumer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerRelease_(Consumer)
		
	ConsumerReleaseRequest = Новый Структура;
	ConsumerReleaseRequest.Вставить("consumerId", Consumer.id);
	ConsumerReleaseRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HTTPЗапрос("consumer/release");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerReleaseRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerList() Экспорт
		
	HttpОтвет = ConsumerList_();
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerList_()
	
	HttpЗапрос = Новый HttpЗапрос("consumer");
	
	HttpОтвет = HttpСоединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerTouch(Consumer) Экспорт
	
	HttpОтвет = ConsumerTouch_(Consumer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerTouch_(Consumer)
		
	ConsumerTouchRequest = Новый Структура;
	ConsumerTouchRequest.Вставить("consumerId", Consumer.id);
	ConsumerTouchRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/touch");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerTouchRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerListTopics(Consumer, Pattern = Неопределено) Экспорт
		
	HttpОтвет = ConsumerListTopics_(Consumer, Pattern);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerListTopics_(Consumer, Pattern)
		
	ConsumerListTopicsRequest = Новый Структура;
	ConsumerListTopicsRequest.Вставить("consumerId", Consumer.id);
	ConsumerListTopicsRequest.Вставить("token", Consumer.token);
	Если Pattern <> Неопределено Тогда
		ConsumerListTopicsRequest.Вставить("pattern", Pattern);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("consumer/list-topics");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerListTopicsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetPartitions(Consumer, Topic) Экспорт
		
	HttpОтвет = ConsumerGetPartitions_(Consumer, Topic);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetPartitions_(Consumer, Topic)
		
	ConsumerGetPartitionsRequest = Новый Структура;
	ConsumerGetPartitionsRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetPartitionsRequest.Вставить("token", Consumer.token);
	ConsumerGetPartitionsRequest.Вставить("topic", Topic);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-partitions");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetPartitionsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerAssign(Consumer, Partitions) Экспорт
	
	HttpОтвет = ConsumerAssign_(Consumer, Partitions);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerAssign_(Consumer, Partitions)
	
	ConsumerAssignRequest = Новый Структура;
	ConsumerAssignRequest.Вставить("consumerId", Consumer.id);
	ConsumerAssignRequest.Вставить("token", Consumer.token);
	ConsumerAssignRequest.Вставить("partitions", Partitions);
			
	HttpЗапрос = Новый HTTPЗапрос("consumer/assign");
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, ConsumerAssignRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetAssignment(Consumer) Экспорт
		
	HttpОтвет = ConsumerGetAssignment_(Consumer);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetAssignment_(Consumer)
		
	ConsumerGetAssignmentRequest = Новый Структура;
	ConsumerGetAssignmentRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetAssignmentRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-assignment");
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetAssignmentRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerSubscribe(Consumer, TopicsOrPattern) Экспорт
	
	HttpОтвет = ConsumerSubscribe_(Consumer, TopicsOrPattern);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerSubscribe_(Consumer, TopicsOrPattern)
	
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
			
	HttpЗапрос = Новый HTTPЗапрос("consumer/subscribe");
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, ConsumerSubscribeRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetSubscription(Consumer) Экспорт
		
	HttpОтвет = ConsumerGetSubscription_(Consumer);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetSubscription_(Consumer)
		
	ConsumerGetSubscriptionRequest = Новый Структура;
	ConsumerGetSubscriptionRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetSubscriptionRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-subscription");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetSubscriptionRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerUnsubscribe(Consumer) Экспорт
	
	HttpОтвет = ConsumerUnsubscribe_(Consumer);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerUnsubscribe_(Consumer)
		
	ConsumerUnsubscribeRequest = Новый Структура;
	ConsumerUnsubscribeRequest.Вставить("consumerId", Consumer.id);
	ConsumerUnsubscribeRequest.Вставить("token", Consumer.token);
			
	HttpЗапрос = Новый HTTPЗапрос("consumer/unsubscribe");
	
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, ConsumerUnsubscribeRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerSeek(Consumer, Topic, Partition, Offset) Экспорт
	
	HttpОтвет = ConsumerSeek_(Consumer, Topic, Partition, Offset);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerSeek_(Consumer, Topic, Partition, Offset)
	
	ConsumerSeekRequest = Новый Структура;
	ConsumerSeekRequest.Вставить("consumerId", Consumer.id);
	ConsumerSeekRequest.Вставить("token", Consumer.token);
	ConsumerSeekRequest.Вставить("topic", Topic);
	ConsumerSeekRequest.Вставить("partition", Partition);
	ConsumerSeekRequest.Вставить("offset", Offset);
			
	HttpЗапрос = Новый HTTPЗапрос("consumer/seek");
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, ConsumerSeekRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerSeekToBeginning(Consumer, Partitions) Экспорт
	
	HttpОтвет = ConsumerSeekToBeginning_(Consumer, Partitions);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerSeekToBeginning_(Consumer, Partitions)
	
	ConsumerSeekToRequest = Новый Структура;
	ConsumerSeekToRequest.Вставить("consumerId", Consumer.id);
	ConsumerSeekToRequest.Вставить("token", Consumer.token);
	ConsumerSeekToRequest.Вставить("partitions", Partitions);
			
	HttpЗапрос = Новый HTTPЗапрос("consumer/seek-to-beginning");
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, ConsumerSeekToRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerSeekToEnd(Consumer, Partitions) Экспорт
	
	HttpОтвет = ConsumerSeekToEnd_(Consumer, Partitions);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerSeekToEnd_(Consumer, Partitions)
	
	ConsumerSeekToRequest = Новый Структура;
	ConsumerSeekToRequest.Вставить("consumerId", Consumer.id);
	ConsumerSeekToRequest.Вставить("token", Consumer.token);
	ConsumerSeekToRequest.Вставить("partitions", Partitions);
			
	HttpЗапрос = Новый HTTPЗапрос("consumer/seek-to-end");
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, ConsumerSeekToRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetPosition(Consumer, Topic, Partition) Экспорт
		
	HttpОтвет = ConsumerGetPosition_(Consumer, Topic, Partition);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetPosition_(Consumer, Topic, Partition)
		
	ConsumerGetPositionRequest = Новый Структура;
	ConsumerGetPositionRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetPositionRequest.Вставить("token", Consumer.token);
	ConsumerGetPositionRequest.Вставить("topic", Topic);
	ConsumerGetPositionRequest.Вставить("partition", Partition);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-position");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetPositionRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetBeginningOffsets(Consumer, Partitions) Экспорт
		
	HttpОтвет = ConsumerGetBeginningOffsets_(Consumer, Partitions);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetBeginningOffsets_(Consumer, Partitions)
		
	ConsumerGetOffsetsRequest = Новый Структура;
	ConsumerGetOffsetsRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetOffsetsRequest.Вставить("token", Consumer.token);
	ConsumerGetOffsetsRequest.Вставить("partitions", Partitions);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-beginning-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetEndOffsets(Consumer, Partitions) Экспорт
		
	HttpОтвет = ConsumerGetEndOffsets_(Consumer, Partitions);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetEndOffsets_(Consumer, Partitions)
		
	ConsumerGetOffsetsRequest = Новый Структура;
	ConsumerGetOffsetsRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetOffsetsRequest.Вставить("token", Consumer.token);
	ConsumerGetOffsetsRequest.Вставить("partitions", Partitions);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-end-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
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
	
	HttpЗапрос = Новый HttpЗапрос("consumer/poll");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerPollRequest);
	
	Если Accept <> Неопределено Тогда
		HttpЗапрос.Заголовки.Вставить("Accept", Accept);
	КонецЕсли;
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerCommit(Consumer) Экспорт
		
	HttpОтвет = ConsumerCommit_(Consumer);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerCommit_(Consumer)
		
	ConsumerCommitRequest = Новый Структура;
	ConsumerCommitRequest.Вставить("consumerId", Consumer.id);
	ConsumerCommitRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/commit");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerCommitRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetCommitted(Consumer, Partitions) Экспорт
		
	HttpОтвет = ConsumerGetCommitted_(Consumer, Partitions);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetCommitted_(Consumer, Partitions)
		
	ConsumerGetCommittedRequest = Новый Структура;
	ConsumerGetCommittedRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetCommittedRequest.Вставить("token", Consumer.token);
	ConsumerGetCommittedRequest.Вставить("partitions", Partitions);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-committed");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetCommittedRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetGroupMetadata(Consumer) Экспорт
		
	HttpОтвет = ConsumerGetGroupMetadata_(Consumer);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetGroupMetadata_(Consumer)
		
	ConsumerGetGroupMetadataRequest = Новый Структура;
	ConsumerGetGroupMetadataRequest.Вставить("consumerId", Consumer.id);
	ConsumerGetGroupMetadataRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/get-group-metadata");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerGetGroupMetadataRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

#КонецОбласти

#Область Администрирование

Функция AdminCreate(Name, Config, Знач ExpirationTimeout = Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	HttpОтвет = AdminCreate_(Name, Config, ExpirationTimeout);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminCreate_(Name, Config, ExpirationTimeout)
		
	AdminCreateRequest = Новый Структура;
	AdminCreateRequest.Вставить("name", Name);
	AdminCreateRequest.Вставить("config", Config);
	AdminCreateRequest.Вставить("expirationTimeout", ExpirationTimeout);
	
	HttpЗапрос = Новый HttpЗапрос("admin/create");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminCreateRequest);

	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminRelease(Admin) Экспорт
			
	HttpОтвет = AdminRelease_(Admin);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminRelease_(Admin)
		
	AdminReleaseRequest = Новый Структура;
	AdminReleaseRequest.Вставить("adminId", Admin.id);
	AdminReleaseRequest.Вставить("token", Admin.token);
	
	HttpЗапрос = Новый HTTPЗапрос("admin/release");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminReleaseRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminList() Экспорт
		
	HttpОтвет = AdminList_();
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminList_()
	
	HttpЗапрос = Новый HttpЗапрос("admin");
	
	HttpОтвет = HttpСоединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminTouch(Admin) Экспорт
	
	HttpОтвет = AdminTouch_(Admin);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminTouch_(Admin)
		
	AdminTouchRequest = Новый Структура;
	AdminTouchRequest.Вставить("adminId", Admin.id);
	AdminTouchRequest.Вставить("token", Admin.token);
	
	HttpЗапрос = Новый HttpЗапрос("admin/touch");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminTouchRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeCluster(Admin) Экспорт
		
	HttpОтвет = AdminDescribeCluster_(Admin);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeCluster_(Admin)
		
	AdminDescribeClusterRequest = Новый Структура;
	AdminDescribeClusterRequest.Вставить("adminId", Admin.id);
	AdminDescribeClusterRequest.Вставить("token", Admin.token);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-cluster");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeClusterRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeLogDirs(Admin, BrokerIds) Экспорт
		
	HttpОтвет = AdminDescribeLogDirs_(Admin, BrokerIds);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeLogDirs_(Admin, BrokerIds)
		
	AdminDescribeLogDirsRequest = Новый Структура;
	AdminDescribeLogDirsRequest.Вставить("adminId", Admin.id);
	AdminDescribeLogDirsRequest.Вставить("token", Admin.token);
	AdminDescribeLogDirsRequest.Вставить("brokerIds", BrokerIds);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-log-dirs");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeLogDirsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListTopics(Admin, IncludeInternal = Неопределено, Pattern = Неопределено) Экспорт
		
	HttpОтвет = AdminListTopics_(Admin, IncludeInternal, Pattern);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListTopics_(Admin, IncludeInternal, Pattern)
		
	AdminListTopicsRequest = Новый Структура;
	AdminListTopicsRequest.Вставить("adminId", Admin.id);
	AdminListTopicsRequest.Вставить("token", Admin.token);
	Если IncludeInternal <> Неопределено Тогда
		AdminListTopicsRequest.Вставить("includeInternal", IncludeInternal);
	КонецЕсли;
	Если Pattern <> Неопределено Тогда
		AdminListTopicsRequest.Вставить("pattern", Pattern);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-topics");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListTopicsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminCreateTopic(Admin, TopicName, NumPartitions = Неопределено, ReplicationFactor = Неопределено) Экспорт
	
	HttpОтвет = AdminCreateTopic_(Admin, TopicName, NumPartitions, ReplicationFactor);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminCreateTopic_(Admin, TopicName, NumPartitions, ReplicationFactor)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/create-topic");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminCreateTopicRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminCreateTopics(Admin, Topics) Экспорт
	
	HttpОтвет = AdminCreateTopics_(Admin, Topics);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminCreateTopics_(Admin, Topics)
		
	AdminCreateTopicsRequest = Новый Структура;
	AdminCreateTopicsRequest.Вставить("adminId", Admin.id);
	AdminCreateTopicsRequest.Вставить("token", Admin.token);
	AdminCreateTopicsRequest.Вставить("topics", Topics);
	
	HttpЗапрос = Новый HttpЗапрос("admin/create-topics");
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminCreateTopicsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteTopic(Admin, TopicName) Экспорт
	
	HttpОтвет = AdminDeleteTopic_(Admin, TopicName);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteTopic_(Admin, TopicName)
		
	AdminDeleteTopicRequest = Новый Структура;
	AdminDeleteTopicRequest.Вставить("adminId", Admin.id);
	AdminDeleteTopicRequest.Вставить("token", Admin.token);
	AdminDeleteTopicRequest.Вставить("topicName", TopicName);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-topic");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteTopicRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteTopics(Admin, TopicNames) Экспорт
	
	HttpОтвет = AdminDeleteTopics_(Admin, TopicNames);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDeleteTopics_(Admin, TopicNames)
		
	AdminDeleteTopicsRequest = Новый Структура;
	AdminDeleteTopicsRequest.Вставить("adminId", Admin.id);
	AdminDeleteTopicsRequest.Вставить("token", Admin.token);
	AdminDeleteTopicsRequest.Вставить("topicNames", TopicNames);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-topics");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteTopicsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteRecords(Admin, Topic, Partition, BeforeOffset) Экспорт
	
	HttpОтвет = AdminDeleteRecords_(Admin, Topic, Partition, BeforeOffset);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDeleteRecords_(Admin, Topic, Partition, BeforeOffset)
		
	AdminDeleteRecordsRequest = Новый Структура;
	AdminDeleteRecordsRequest.Вставить("adminId", Admin.id);
	AdminDeleteRecordsRequest.Вставить("token", Admin.token);
	AdminDeleteRecordsRequest.Вставить("topic", Topic);
	AdminDeleteRecordsRequest.Вставить("partition", Partition);
	AdminDeleteRecordsRequest.Вставить("beforeOffset", BeforeOffset);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-records");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteRecordsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeTopic(Admin, TopicName, IncludeAuthorizedOperations = Неопределено) Экспорт
		
	HttpОтвет = AdminDescribeTopic_(Admin, TopicName, IncludeAuthorizedOperations);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeTopic_(Admin, TopicName, IncludeAuthorizedOperations)
		
	AdminDescribeTopicRequest = Новый Структура;
	AdminDescribeTopicRequest.Вставить("adminId", Admin.id);
	AdminDescribeTopicRequest.Вставить("token", Admin.token);
	AdminDescribeTopicRequest.Вставить("topicName", TopicName);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeTopicRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-topic");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeTopicRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminCreatePartitions(Admin, Topic, IncreaseTo) Экспорт
		
	HttpОтвет = AdminCreatePartitions_(Admin, Topic, IncreaseTo);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminCreatePartitions_(Admin, Topic, IncreaseTo)
		
	AdminCreatePartitionsRequest = Новый Структура;
	AdminCreatePartitionsRequest.Вставить("adminId", Admin.id);
	AdminCreatePartitionsRequest.Вставить("token", Admin.token);
	AdminCreatePartitionsRequest.Вставить("topicName", Topic);
	AdminCreatePartitionsRequest.Вставить("increaseTo", IncreaseTo);
	
	HttpЗапрос = Новый HttpЗапрос("admin/create-partitions");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminCreatePartitionsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeBrokerConfigs(Admin, BrokerId) Экспорт
		
	HttpОтвет = AdminDescribeBrokerConfigs_(Admin, BrokerId);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeBrokerConfigs_(Admin, BrokerId)
		
	AdminDescribeBrokerConfigsRequest = Новый Структура;
	AdminDescribeBrokerConfigsRequest.Вставить("adminId", Admin.id);
	AdminDescribeBrokerConfigsRequest.Вставить("token", Admin.token);
	AdminDescribeBrokerConfigsRequest.Вставить("brokerId", BrokerId);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-broker-configs");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeBrokerConfigsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeTopicConfigs(Admin, TopicName) Экспорт
	
	HttpОтвет = AdminDescribeTopicConfigs_(Admin, TopicName);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeTopicConfigs_(Admin, TopicName)
		
	AdminDescribeTopicConfigsRequest = Новый Структура;
	AdminDescribeTopicConfigsRequest.Вставить("adminId", Admin.id);
	AdminDescribeTopicConfigsRequest.Вставить("token", Admin.token);
	AdminDescribeTopicConfigsRequest.Вставить("topicName", TopicName);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-topic-configs");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeTopicConfigsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeGroupConfigs(Admin, GroupId) Экспорт
	
	HttpОтвет = AdminDescribeGroupConfigs_(Admin, GroupId);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeGroupConfigs_(Admin, GroupId)
		
	AdminDescribeGroupConfigsRequest = Новый Структура;
	AdminDescribeGroupConfigsRequest.Вставить("adminId", Admin.id);
	AdminDescribeGroupConfigsRequest.Вставить("token", Admin.token);
	AdminDescribeGroupConfigsRequest.Вставить("groupId", GroupId);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-group-configs");
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeGroupConfigsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminAlterTopicConfig(Admin, TopicName, ConfigName, NewValue) Экспорт
		
	HttpОтвет = AdminAlterTopicConfig_(Admin, TopicName, ConfigName, NewValue);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminAlterTopicConfig_(Admin, TopicName, ConfigName, NewValue)
		
	AdminAlterTopicConfigRequest = Новый Структура;
	AdminAlterTopicConfigRequest.Вставить("adminId", Admin.id);
	AdminAlterTopicConfigRequest.Вставить("token", Admin.token);
	AdminAlterTopicConfigRequest.Вставить("topicName", TopicName);
	AdminAlterTopicConfigRequest.Вставить("configName", ConfigName);
	AdminAlterTopicConfigRequest.Вставить("newValue", NewValue);
	
	HttpЗапрос = Новый HttpЗапрос("admin/alter-topic-config");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminAlterTopicConfigRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminAlterGroupConfig(Admin, GroupId, ConfigName, NewValue) Экспорт
		
	HttpОтвет = AdminAlterGroupConfig_(Admin, GroupId, ConfigName, NewValue);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminAlterGroupConfig_(Admin, GroupId, ConfigName, NewValue)
		
	AdminAlterGroupConfigRequest = Новый Структура;
	AdminAlterGroupConfigRequest.Вставить("adminId", Admin.id);
	AdminAlterGroupConfigRequest.Вставить("token", Admin.token);
	AdminAlterGroupConfigRequest.Вставить("groupId", GroupId);
	AdminAlterGroupConfigRequest.Вставить("configName", ConfigName);
	AdminAlterGroupConfigRequest.Вставить("newValue", NewValue);
	
	HttpЗапрос = Новый HttpЗапрос("admin/alter-group-config");
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminAlterGroupConfigRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteTopicConfig(Admin, TopicName, ConfigName) Экспорт
		
	HttpОтвет = AdminDeleteTopicConfig_(Admin, TopicName, ConfigName);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteTopicConfig_(Admin, TopicName, ConfigName)
		
	AdminDeleteTopicConfigRequest = Новый Структура;
	AdminDeleteTopicConfigRequest.Вставить("adminId", Admin.id);
	AdminDeleteTopicConfigRequest.Вставить("token", Admin.token);
	AdminDeleteTopicConfigRequest.Вставить("topicName", TopicName);
	AdminDeleteTopicConfigRequest.Вставить("configName", ConfigName);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-topic-config");
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteTopicConfigRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteGroupConfig(Admin, GroupId, ConfigName) Экспорт
		
	HttpОтвет = AdminDeleteGroupConfig_(Admin, GroupId, ConfigName);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteGroupConfig_(Admin, GroupId, ConfigName)
		
	AdminDeleteGroupConfigRequest = Новый Структура;
	AdminDeleteGroupConfigRequest.Вставить("adminId", Admin.id);
	AdminDeleteGroupConfigRequest.Вставить("token", Admin.token);
	AdminDeleteGroupConfigRequest.Вставить("groupId", GroupId);
	AdminDeleteGroupConfigRequest.Вставить("configName", ConfigName);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-group-config");
	
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteGroupConfigRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeUserScramCredentials(Admin, Users = Неопределено) Экспорт
	
	HttpОтвет = AdminDescribeUserScramCredentials_(Admin, Users);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeUserScramCredentials_(Admin, Users)
		
	AdminDescribeUserScramCredentialsRequest = Новый Структура;
	AdminDescribeUserScramCredentialsRequest.Вставить("adminId", Admin.id);
	AdminDescribeUserScramCredentialsRequest.Вставить("token", Admin.token);
	Если Users <> Неопределено Тогда
		AdminDescribeUserScramCredentialsRequest.Вставить("users", Users);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-user-scram-credentials");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeUserScramCredentialsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminUpsertUserScramCredentials(Admin, User, Mechanism, Password, Iterations = Неопределено) Экспорт
	
	HttpОтвет = AdminUpsertUserScramCredentials_(Admin, User, Mechanism, Password, Iterations);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminUpsertUserScramCredentials_(Admin, User, Mechanism, Password, Iterations)
		
	AdminUpsertUserScramCredentialsRequest = Новый Структура;
	AdminUpsertUserScramCredentialsRequest.Вставить("adminId", Admin.id);
	AdminUpsertUserScramCredentialsRequest.Вставить("token", Admin.token);
	AdminUpsertUserScramCredentialsRequest.Вставить("user", User);
	AdminUpsertUserScramCredentialsRequest.Вставить("mechanism", Mechanism);
	AdminUpsertUserScramCredentialsRequest.Вставить("password", Password);
	Если Iterations <> Неопределено Тогда
		AdminUpsertUserScramCredentialsRequest.Вставить("iterations", Iterations);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/upsert-user-scram-credentials");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminUpsertUserScramCredentialsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteUserScramCredentials(Admin, User, Mechanism) Экспорт
	
	HttpОтвет = AdminDeleteUserScramCredentials_(Admin, User, Mechanism);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteUserScramCredentials_(Admin, User, Mechanism)
		
	AdminDeleteUserScramCredentialsRequest = Новый Структура;
	AdminDeleteUserScramCredentialsRequest.Вставить("adminId", Admin.id);
	AdminDeleteUserScramCredentialsRequest.Вставить("token", Admin.token);
	AdminDeleteUserScramCredentialsRequest.Вставить("user", User);
	AdminDeleteUserScramCredentialsRequest.Вставить("mechanism", Mechanism);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-user-scram-credentials");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteUserScramCredentialsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeAcls(Admin, AclBindingFilter = Неопределено) Экспорт
	
	HttpОтвет = AdminDescribeAcls_(Admin, AclBindingFilter);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeAcls_(Admin, AclBindingFilter)
		
	AdminDescribeAclsRequest = Новый Структура;
	AdminDescribeAclsRequest.Вставить("adminId", Admin.id);
	AdminDescribeAclsRequest.Вставить("token", Admin.token);
	Если AclBindingFilter <> Неопределено Тогда
		AdminDescribeAclsRequest.Вставить("filter", AclBindingFilter);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-acls");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeAclsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminCreateAcls(Admin, AclBindings) Экспорт
	
	HttpОтвет = AdminCreateAcls_(Admin, AclBindings);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminCreateAcls_(Admin, AclBindings)
		
	AdminCreateAclsRequest = Новый Структура;
	AdminCreateAclsRequest.Вставить("adminId", Admin.id);
	AdminCreateAclsRequest.Вставить("token", Admin.token);
	AdminCreateAclsRequest.Вставить("acls", AclBindings);
	
	HttpЗапрос = Новый HttpЗапрос("admin/create-acls");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminCreateAclsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteAcls(Admin, AclBindingFilters) Экспорт
	
	HttpОтвет = AdminDeleteAcls_(Admin, AclBindingFilters);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteAcls_(Admin, AclBindingFilters)
		
	AdminDeleteAclsRequest = Новый Структура;
	AdminDeleteAclsRequest.Вставить("adminId", Admin.id);
	AdminDeleteAclsRequest.Вставить("token", Admin.token);
	AdminDeleteAclsRequest.Вставить("filters", AclBindingFilters);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-acls");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteAclsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeProducers(Admin, Partitions) Экспорт
	
	HttpОтвет = AdminDescribeProducers_(Admin, Partitions);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeProducers_(Admin, Partitions)
		
	AdminDescribeProducersRequest = Новый Структура;
	AdminDescribeProducersRequest.Вставить("adminId", Admin.id);
	AdminDescribeProducersRequest.Вставить("token", Admin.token);
	AdminDescribeProducersRequest.Вставить("partitions", Partitions);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-producers");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeProducersRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminAbortTransaction(Admin, TopicPartition, ProducerId, ProducerEpoch, CoordinatorEpoch) Экспорт
	
	HttpОтвет = AdminAbortTransaction_(Admin, TopicPartition, ProducerId, ProducerEpoch, CoordinatorEpoch);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminAbortTransaction_(Admin, TopicPartition, ProducerId, ProducerEpoch, CoordinatorEpoch)
		
	AdminAbortTransactionRequest = Новый Структура;
	AdminAbortTransactionRequest.Вставить("adminId", Admin.id);
	AdminAbortTransactionRequest.Вставить("token", Admin.token);
	AdminAbortTransactionRequest.Вставить("partition", TopicPartition);
	AdminAbortTransactionRequest.Вставить("producerId", ProducerId);
	AdminAbortTransactionRequest.Вставить("producerEpoch", ProducerEpoch);
	AdminAbortTransactionRequest.Вставить("coordinatorEpoch", CoordinatorEpoch);
	
	HttpЗапрос = Новый HttpЗапрос("admin/abort-transaction");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminAbortTransactionRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListGroups(Admin, WithTypes = Неопределено, WithProtocolTypes = Неопределено, InStates = Неопределено) Экспорт
	
	HttpОтвет = AdminListGroups_(Admin, WithTypes, WithProtocolTypes, InStates);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListGroups_(Admin, WithTypes, WithProtocolTypes, InStates)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-groups");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListGroupsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeClassicGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	HttpОтвет = AdminDescribeClassicGroup_(Admin, GroupId, IncludeAuthorizedOperations);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeClassicGroup_(Admin, GroupId, IncludeAuthorizedOperations)
		
	AdminDescribeClassicGroupRequest = Новый Структура;
	AdminDescribeClassicGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeClassicGroupRequest.Вставить("token", Admin.token);
	AdminDescribeClassicGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeClassicGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-classic-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeClassicGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeConsumerGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	HttpОтвет = AdminDescribeConsumerGroup_(Admin, GroupId, IncludeAuthorizedOperations);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeConsumerGroup_(Admin, GroupId, IncludeAuthorizedOperations)
		
	AdminDescribeConsumerGroupRequest = Новый Структура;
	AdminDescribeConsumerGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeConsumerGroupRequest.Вставить("token", Admin.token);
	AdminDescribeConsumerGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeConsumerGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-consumer-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeConsumerGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeShareGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	HttpОтвет = AdminDescribeShareGroup_(Admin, GroupId, IncludeAuthorizedOperations);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeShareGroup_(Admin, GroupId, IncludeAuthorizedOperations)
		
	AdminDescribeShareGroupRequest = Новый Структура;
	AdminDescribeShareGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeShareGroupRequest.Вставить("token", Admin.token);
	AdminDescribeShareGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeShareGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-share-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeShareGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeStreamsGroup(Admin, GroupId, IncludeAuthorizedOperations = Неопределено) Экспорт
	
	HttpОтвет = AdminDescribeStreamsGroup_(Admin, GroupId, IncludeAuthorizedOperations);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeStreamsGroup_(Admin, GroupId, IncludeAuthorizedOperations)
		
	AdminDescribeStreamsGroupRequest = Новый Структура;
	AdminDescribeStreamsGroupRequest.Вставить("adminId", Admin.id);
	AdminDescribeStreamsGroupRequest.Вставить("token", Admin.token);
	AdminDescribeStreamsGroupRequest.Вставить("groupId", GroupId);
	Если IncludeAuthorizedOperations <> Неопределено Тогда
		AdminDescribeStreamsGroupRequest.Вставить("includeAuthorizedOperations", IncludeAuthorizedOperations);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-streams-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeStreamsGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListConsumerGroupOffsets(Admin, GroupId, RequireStable = Неопределено) Экспорт
	
	HttpОтвет = AdminListConsumerGroupOffsets_(Admin, GroupId, RequireStable);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListConsumerGroupOffsets_(Admin, GroupId, RequireStable)
		
	AdminListConsumerGroupOffsetsRequest = Новый Структура;
	AdminListConsumerGroupOffsetsRequest.Вставить("adminId", Admin.id);
	AdminListConsumerGroupOffsetsRequest.Вставить("token", Admin.token);
	AdminListConsumerGroupOffsetsRequest.Вставить("groupId", GroupId);
	Если RequireStable <> Неопределено Тогда
		AdminListConsumerGroupOffsetsRequest.Вставить("requireStable", RequireStable);
	КонецЕсли;
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-consumer-group-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListConsumerGroupOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminAlterConsumerGroupOffsets(Admin, GroupId, Offsets) Экспорт
	
	HttpОтвет = AdminAlterConsumerGroupOffsets_(Admin, GroupId, Offsets);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminAlterConsumerGroupOffsets_(Admin, GroupId, Offsets)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/alter-consumer-group-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminAlterConsumerGroupOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteConsumerGroupOffsets(Admin, GroupId, Partitions) Экспорт
	
	HttpОтвет = AdminDeleteConsumerGroupOffsets_(Admin, GroupId, Partitions);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteConsumerGroupOffsets_(Admin, GroupId, Partitions)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-consumer-group-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteConsumerGroupOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminRemoveMembersFromConsumerGroup(Admin, GroupId, Members = Неопределено, Reason = Неопределено) Экспорт
	
	HttpОтвет = AdminRemoveMembersFromConsumerGroup_(Admin, GroupId, Members, Reason);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminRemoveMembersFromConsumerGroup_(Admin, GroupId, Members, Reason)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/remove-members-from-consumer-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminRemoveMembersFromConsumerGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteConsumerGroup(Admin, GroupId) Экспорт
	
	HttpОтвет = AdminDeleteConsumerGroup_(Admin, GroupId);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteConsumerGroup_(Admin, GroupId)
		
	AdminDeleteConsumerGroupRequest = Новый Структура;
	AdminDeleteConsumerGroupRequest.Вставить("adminId", Admin.id);
	AdminDeleteConsumerGroupRequest.Вставить("token", Admin.token);
	AdminDeleteConsumerGroupRequest.Вставить("groupId", GroupId);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-consumer-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteConsumerGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteConsumerGroups(Admin, GroupIds) Экспорт
	
	HttpОтвет = AdminDeleteConsumerGroups_(Admin, GroupIds);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteConsumerGroups_(Admin, GroupIds)
		
	AdminDeleteConsumerGroupsRequest = Новый Структура;
	AdminDeleteConsumerGroupsRequest.Вставить("adminId", Admin.id);
	AdminDeleteConsumerGroupsRequest.Вставить("token", Admin.token);
	AdminDeleteConsumerGroupsRequest.Вставить("groupIds", GroupIds);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-consumer-groups");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteConsumerGroupsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteShareGroup(Admin, GroupId) Экспорт
	
	HttpОтвет = AdminDeleteShareGroup_(Admin, GroupId);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteShareGroup_(Admin, GroupId)
		
	AdminDeleteShareGroupRequest = Новый Структура;
	AdminDeleteShareGroupRequest.Вставить("adminId", Admin.id);
	AdminDeleteShareGroupRequest.Вставить("token", Admin.token);
	AdminDeleteShareGroupRequest.Вставить("groupId", GroupId);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-share-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteShareGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteShareGroups(Admin, GroupIds) Экспорт
	
	HttpОтвет = AdminDeleteShareGroups_(Admin, GroupIds);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteShareGroups_(Admin, GroupIds)
		
	AdminDeleteShareGroupsRequest = Новый Структура;
	AdminDeleteShareGroupsRequest.Вставить("adminId", Admin.id);
	AdminDeleteShareGroupsRequest.Вставить("token", Admin.token);
	AdminDeleteShareGroupsRequest.Вставить("groupIds", GroupIds);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-share-groups");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteShareGroupsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteStreamsGroup(Admin, GroupId) Экспорт
	
	HttpОтвет = AdminDeleteStreamsGroup_(Admin, GroupId);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteStreamsGroup_(Admin, GroupId)
		
	AdminDeleteStreamsGroupRequest = Новый Структура;
	AdminDeleteStreamsGroupRequest.Вставить("adminId", Admin.id);
	AdminDeleteStreamsGroupRequest.Вставить("token", Admin.token);
	AdminDeleteStreamsGroupRequest.Вставить("groupId", GroupId);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-streams-group");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteStreamsGroupRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteStreamsGroups(Admin, GroupIds) Экспорт
	
	HttpОтвет = AdminDeleteStreamsGroups_(Admin, GroupIds);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminDeleteStreamsGroups_(Admin, GroupIds)
		
	AdminDeleteStreamsGroupsRequest = Новый Структура;
	AdminDeleteStreamsGroupsRequest.Вставить("adminId", Admin.id);
	AdminDeleteStreamsGroupsRequest.Вставить("token", Admin.token);
	AdminDeleteStreamsGroupsRequest.Вставить("groupIds", GroupIds);
	
	HttpЗапрос = Новый HttpЗапрос("admin/delete-streams-groups");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDeleteStreamsGroupsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListEarliestOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	HttpОтвет = AdminListEarliestOffsets_(Admin, Partitions, IsolationLevel);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListEarliestOffsets_(Admin, Partitions, IsolationLevel)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-earliest-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListEarliestLocalOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	HttpОтвет = AdminListEarliestLocalOffsets_(Admin, Partitions, IsolationLevel);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListEarliestLocalOffsets_(Admin, Partitions, IsolationLevel)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-earliest-local-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListLatestOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	HttpОтвет = AdminListLatestOffsets_(Admin, Partitions, IsolationLevel);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListLatestOffsets_(Admin, Partitions, IsolationLevel)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-latest-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListLatestTieredOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	HttpОтвет = AdminListLatestTieredOffsets_(Admin, Partitions, IsolationLevel);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListLatestTieredOffsets_(Admin, Partitions, IsolationLevel)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-latest-tiered-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListMaxTimestampOffsets(Admin, Partitions, IsolationLevel = Неопределено) Экспорт
	
	HttpОтвет = AdminListMaxTimestampOffsets_(Admin, Partitions, IsolationLevel);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListMaxTimestampOffsets_(Admin, Partitions, IsolationLevel)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-max-timestamp-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminListTimestampOffsets(Admin, Partitions, Timestamp, IsolationLevel = Неопределено) Экспорт
	
	HttpОтвет = AdminListTimestampOffsets_(Admin, Partitions, Timestamp, IsolationLevel);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListTimestampOffsets_(Admin, Partitions, Timestamp, IsolationLevel)
		
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
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-timestamp-offsets");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListOffsetsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

#КонецОбласти

Функция GetVersion() Экспорт
		
	HttpОтвет = GetVersion_();
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция GetVersion_()
	
	HttpЗапрос = Новый HttpЗапрос("version");
	
	HttpОтвет = HttpСоединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
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

