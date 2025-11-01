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
// Низкоуровневый API интеграции Apache Kafka и 1C:Предприятие через
// REST-прокси.
// Версия 5.0.
// Автор: Сергей Савельев.
// Репозиторий REST-прокси: https://github.com/SergeSavel/kafka-proxy-java
///////////////////////////////////////////////////////////////////////////////

#Область Переменные

Перем HttpСоединение;
Перем ПараметрыЗаписиJson;

#КонецОбласти

#Область Инициализация

Процедура Инициализировать(ПроксиАдрес = Неопределено, ПроксиПользователь = Неопределено, ПроксиПароль = Неопределено, ПроксиТаймаут = 65, ЗащищенноеСоединение = Неопределено) Экспорт
		
	Если ПроксиАдрес = Неопределено Или ПустаяСтрока(ПроксиАдрес) Тогда
		ПроксиСервер = "localhost";
		ПроксиПорт = 8086;
	Иначе
		ПозДвоеточие = СтрНайти(ПроксиАдрес, ":");
		Если ПозДвоеточие = 0 Тогда
			ПроксиСервер = ПроксиАдрес;
			ПроксиПорт = 8086;
		Иначе
			ПроксиСервер = Лев(ПроксиАдрес, ПозДвоеточие-1);
			ПроксиПорт = Число(Сред(ПроксиАдрес, ПозДвоеточие+1));
		КонецЕсли;
	КонецЕсли;
	
	Если ЗащищенноеСоединение = Неопределено Тогда	
		HttpСоединение = Новый HttpСоединение(ПроксиСервер, ПроксиПорт, ПроксиПользователь, ПроксиПароль, , ПроксиТаймаут);
	Иначе
		HttpСоединение = Новый HttpСоединение(ПроксиСервер, ПроксиПорт, ПроксиПользователь, ПроксиПароль, , ПроксиТаймаут, ЗащищенноеСоединение);
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

Функция ProducerSend(Producer, Topic, KeyString=Неопределено, ValueString, Headers=Неопределено) Экспорт
	
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

#КонецОбласти

#Область Получение

Функция ConsumerCreate(Name, Config, Знач ExpirationTimeout=Неопределено) Экспорт
	
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

Функция ConsumerListTopics(Consumer) Экспорт
		
	HttpОтвет = ConsumerListTopics_(Consumer);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerListTopics_(Consumer)
		
	ConsumerListTopicsRequest = Новый Структура;
	ConsumerListTopicsRequest.Вставить("consumerId", Consumer.id);
	ConsumerListTopicsRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/list-topics");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerListTopicsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerListPartitions(Consumer, Topic) Экспорт
		
	HttpОтвет = ConsumerListPartitions_(Consumer, Topic);
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerListPartitions_(Consumer, Topic)
		
	ConsumerListPartitionsRequest = Новый Структура;
	ConsumerListPartitionsRequest.Вставить("consumerId", Consumer.id);
	ConsumerListPartitionsRequest.Вставить("token", Consumer.token);
	ConsumerListPartitionsRequest.Вставить("topic", Topic);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/list-partitions");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerListPartitionsRequest);
	
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
	Иначе
		ConsumerSubscribeRequest.Вставить("pattern", TopicsOrPattern);
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
		
	Результат = ПрочитатьТелоHttpОтвета(HttpОтвет, Истина);
	
	Если Результат = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
		
	Для Каждого КлючЗначение Из Результат Цикл
		Массив = КлючЗначение.Значение;
		ВГраница = Массив.ВГраница();
		Для Индекс = 0 По ВГраница Цикл
			Структура = Новый Структура;
			Соответствие = Массив[Индекс];
			Для Каждого КлючЗначение1 Из Соответствие Цикл
				Структура.Вставить(КлючЗначение1.Ключ, КлючЗначение1.Значение);
			КонецЦикла;
			Массив[Индекс] = Структура;
		КонецЦикла;
	КонецЦикла;
	
	Возврат Результат;
	
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
		
	Результат = ПрочитатьТелоHttpОтвета(HttpОтвет, Истина);
	
	Если Результат = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
		
	Для Каждого КлючЗначение Из Результат Цикл
		Массив = КлючЗначение.Значение;
		ВГраница = Массив.ВГраница();
		Для Индекс = 0 По ВГраница Цикл
			Структура = Новый Структура;
			Соответствие = Массив[Индекс];
			Для Каждого КлючЗначение1 Из Соответствие Цикл
				Структура.Вставить(КлючЗначение1.Ключ, КлючЗначение1.Значение);
			КонецЦикла;
			Массив[Индекс] = Структура;
		КонецЦикла;
	КонецЦикла;
	
	Возврат Результат;
	
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
Функция ConsumerPollRaw(Consumer, Timeout, Accept=Неопределено) Экспорт
		
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
		
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerCommit_(Consumer)
		
	ConsumerCommitRequest = Новый Структура;
	ConsumerCommitRequest.Вставить("consumerId", Consumer.id);
	ConsumerCommitRequest.Вставить("token", Consumer.token);
	
	HttpЗапрос = Новый HttpЗапрос("consumer/poll");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, ConsumerCommitRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

#КонецОбласти

#Область Администрирование

Функция AdminCreate(Name, Config, Знач ExpirationTimeout=Неопределено) Экспорт
	
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

Функция AdminListTopics(Admin) Экспорт
		
	HttpОтвет = AdminListTopics_(Admin);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminListTopics_(Admin)
		
	AdminListTopicsRequest = Новый Структура;
	AdminListTopicsRequest.Вставить("adminId", Admin.id);
	AdminListTopicsRequest.Вставить("token", Admin.token);
	
	HttpЗапрос = Новый HttpЗапрос("admin/list-topics");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminListTopicsRequest);
	
	HttpОтвет = HttpСоединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьHttpОтвет(HttpОтвет);
	
КонецФункции

Функция AdminCreateTopic(Admin, TopicName, NumPartitions = Неопределено, ReplicationFactor = Неопределено) Экспорт
	
	HttpОтвет = AdminCreateTopic_(Admin, TopicName, NumPartitions, ReplicationFactor);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
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

Функция AdminDescribeTopic(Admin, Topic) Экспорт
		
	HttpОтвет = AdminDescribeTopic_(Admin, Topic);
	
	Возврат ПрочитатьТелоHttpОтвета(HttpОтвет);
	
КонецФункции
Функция AdminDescribeTopic_(Admin, Topic)
		
	AdminDescribeTopicRequest = Новый Структура;
	AdminDescribeTopicRequest.Вставить("adminId", Admin.id);
	AdminDescribeTopicRequest.Вставить("token", Admin.token);
	AdminDescribeTopicRequest.Вставить("topic", Topic);
	
	HttpЗапрос = Новый HttpЗапрос("admin/describe-topic");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminDescribeTopicRequest);
	
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

Функция AdminSetTopicConfig(Admin, TopicName, ConfigName, NewValue) Экспорт
		
	HttpОтвет = AdminSetTopicConfig_(Admin, TopicName, ConfigName, NewValue);
	
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminSetTopicConfig_(Admin, TopicName, ConfigName, NewValue)
		
	AdminSetTopicConfigRequest = Новый Структура;
	AdminSetTopicConfigRequest.Вставить("adminId", Admin.id);
	AdminSetTopicConfigRequest.Вставить("token", Admin.token);
	AdminSetTopicConfigRequest.Вставить("topicName", TopicName);
	AdminSetTopicConfigRequest.Вставить("configName", ConfigName);
	AdminSetTopicConfigRequest.Вставить("newValue", NewValue);
	
	HttpЗапрос = Новый HttpЗапрос("admin/set-topic-config");
	ЗаписатьJsonВHttpЗапрос(HttpЗапрос, AdminSetTopicConfigRequest);
	
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

Функция ПрочитатьТелоHttpОтвета(HttpОтвет, КакСоответствие = Ложь)
	
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
		Результат = ПрочитатьJson(ЧтениеJson, КакСоответствие);
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

