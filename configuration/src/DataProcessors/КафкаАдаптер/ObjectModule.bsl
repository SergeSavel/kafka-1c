// Copyright 2023 Савельев Сергей Владимирович
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
// Версия 4.1.
// Автор: Сергей Савельев.
// Репозиторий REST-прокси: https://github.com/SergeSavel/kafka-rest-proxy-dotnet
///////////////////////////////////////////////////////////////////////////////

Перем Соединение;
Перем ПараметрыЗаписиJSON;

Процедура Инициализировать(Знач ПроксиАдрес=Неопределено, ПроксиПользователь=Неопределено, ПроксиПароль=Неопределено, ПроксиТаймаут=65) Экспорт
		
	Если ПроксиАдрес=Неопределено Или ПустаяСтрока(ПроксиАдрес) Тогда
		_Сервер = "localhost";
		_Порт = "8086";
	Иначе
		ПозДвоеточие = СтрНайти(ПроксиАдрес, ":");
		Если ПозДвоеточие = 0 Тогда
			_Сервер = ПроксиАдрес;
			_Порт = "";
		Иначе
			_Сервер = Лев(ПроксиАдрес, ПозДвоеточие-1);
			_Порт = Сред(ПроксиАдрес, ПозДвоеточие+1);
		КонецЕсли;
	КонецЕсли;
		
	Если ПустаяСтрока(_Порт) Тогда	
		Соединение = Новый HTTPСоединение(_Сервер, , ПроксиПользователь, ПроксиПароль, , ПроксиТаймаут);
	Иначе
		Соединение = Новый HTTPСоединение(_Сервер, Число(_Порт), ПроксиПользователь, ПроксиПароль, , ПроксиТаймаут);
	КонецЕсли;
	
	ПараметрыЗаписиJSON = Новый ПараметрыЗаписиJSON(ПереносСтрокJSON.Нет);
		
КонецПроцедуры

#Область Администрирование

Функция AdminCreate(Name, Config, Знач ExpirationTimeout=Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	HttpОтвет = AdminCreate_(Name, Config, ExpirationTimeout);
	
	Admin = ПрочитатьТелоОтвета(HttpОтвет);
	
	//Если Admin = Неопределено Тогда
	//	Возврат Неопределено;
	//КонецЕсли;
		
	Возврат Admin;
	
КонецФункции
Функция AdminCreate_(Name, Config, ExpirationTimeout)
	
	Body = Новый Структура;
	Body.Вставить("Name", Name);
	Body.Вставить("Config", Config);
	Body.Вставить("ExpirationTimeoutMs", ExpirationTimeout);
	
	HttpЗапрос = Новый HTTPЗапрос("admin");
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Body);

	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция AdminRelease(AdminId, Token) Экспорт
	
	// Если уже была попытка удалить экземаляр, вторую попытку не производим.
	Если AdminId = Null Тогда
		Возврат Истина;
	КонецЕсли;
	AdminId_ = AdminId;
	AdminId = Null;
	
	КодОтвета_ = КодОтвета;
	ОписаниеОшибки_ = ОписаниеОшибки;
	
	HttpОтвет = AdminRelease_(AdminId_, Token);
	
	// ???
	Если КодОтвета_<200 Или КодОтвета_>299 Тогда
		КодОтвета = КодОтвета_;
		ОписаниеОшибки = ОписаниеОшибки_;
	КонецЕсли;
	
	Возврат (HttpОтвет<>Неопределено);
	
КонецФункции
Функция AdminRelease_(AdminId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("admin/"+AdminId+"?token="+Token);
		
	HttpОтвет = Соединение.Удалить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция AdminGet(AdminId) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	HttpОтвет = AdminGet_(AdminId);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция AdminGet_(AdminId)
	
	HttpЗапрос = Новый HTTPЗапрос("admin/"+AdminId);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция AdminTouch(AdminId, Token) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр отправителя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = AdminTouch_(AdminId, Token);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция AdminTouch_(AdminId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("admin/"+AdminId+"/touch?token="+Token);
		
	HttpОтвет = Соединение.Записать(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция AdminGetMetadata(AdminId, Token, Topic=Неопределено, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminGetMetadata_(AdminId, Token, Topic, Timeout);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция AdminGetMetadata_(AdminId, Token, Topic, Timeout)
	
	АдресРесурса = "admin/"+AdminId+"/metadata?token="+Token;
	Если Topic <> Неопределено Тогда
		АдресРесурса = АдресРесурса + "&topic="+Topic;
	КонецЕсли;
	АдресРесурса = АдресРесурса + "&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция AdminGetTopicConfig(AdminId, Token, Topic, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminGetTopicConfig_(AdminId, Token, Topic, Timeout);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет, Истина);
	
КонецФункции
Функция AdminGetTopicConfig_(AdminId, Token, Topic, Timeout)
	
	АдресРесурса = "admin/"+AdminId+"/config?token="+Token+"&topic="+Topic+"&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция AdminGetBrokerConfig(AdminId, Token, BrokerId, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminGetBrokerConfig_(AdminId, Token, BrokerId, Timeout);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет, Истина);
	
КонецФункции
Функция AdminGetBrokerConfig_(AdminId, Token, BrokerId, Timeout)
	
	АдресРесурса = "admin/"+AdminId+"/config?token="+Token+"&broker="+BrokerId+"&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция AdminCreateTopic(AdminId, Token, Topic, NumPartitions=Неопределено, ReplicationFactor=Неопределено, Config=Неопределено, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminCreateTopic_(AdminId, Token, Topic, NumPartitions, ReplicationFactor, Config, Timeout);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция AdminCreateTopic_(AdminId, Token, Topic, NumPartitions, ReplicationFactor, Config, Timeout)
	
	Тело = Новый Структура;
	Тело.Вставить("Topic", Topic);
	Если NumPartitions <> Неопределено Тогда
		Тело.Вставить("NumPartitions", NumPartitions);
	КонецЕсли;
	Если ReplicationFactor <> Неопределено Тогда
		Тело.Вставить("ReplicationFactor", ReplicationFactor);
	КонецЕсли;
	Если Config <> Неопределено Тогда
		Тело.Вставить("Config", Config);
	КонецЕсли;
	
	АдресРесурса = "admin/"+AdminId+"/metadata?token="+Token+"&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Тело);
	
	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция AdminCreateAcls(AdminId, Token, AclBindings, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminCreateAcls_(AdminId, Token, AclBindings, Timeout);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция AdminCreateAcls_(AdminId, Token, AclBindings, Timeout)
	
	Тело = Новый Структура;
	Тело.Вставить("AclBindings", AclBindings);
	
	АдресРесурса = "admin/"+AdminId+"/acls/create?token="+Token+"&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Тело);
	
	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDescribeAcls(AdminId, Token, AclBindingFilter, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminDescribeAcls_(AdminId, Token, AclBindingFilter, Timeout);
	
	Ответ = ПрочитатьТелоОтвета(HttpОтвет);
	
	Возврат Ответ.AclBindings;
	
КонецФункции
Функция AdminDescribeAcls_(AdminId, Token, AclBindingFilter, Timeout)
	
	Тело = Новый Структура;
	Тело.Вставить("AclBindingFilter", AclBindingFilter);
	
	АдресРесурса = "admin/"+AdminId+"/acls/describe?token="+Token+"&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Тело);
	
	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция AdminDeleteAcls(AdminId, Token, AclBindingFilters, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(AdminId) Тогда
		ВызватьИсключение "Не заполнен Id администратора. Возможно, экземпляр администратора уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = AdminDeleteAcls_(AdminId, Token, AclBindingFilters, Timeout);
	
	Ответ = ПрочитатьТелоОтвета(HttpОтвет);
	
	Если Ответ <> Неопределено Тогда
		Возврат Ответ.Results;
	КонецЕсли;
	
КонецФункции
Функция AdminDeleteAcls_(AdminId, Token, AclBindingFilters, Timeout)
	
	Тело = Новый Структура;
	Тело.Вставить("AclBindingFilters", AclBindingFilters);
	
	АдресРесурса = "admin/"+AdminId+"/acls/delete?token="+Token+"&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Тело);
	
	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

// Формирует готовую структуру AclBinding для использования в функции AdminCreateAcls 
//
// Параметры:
//  ResourceType	- Строка - Тип ресурса: Any, Topic, Group, Broker.
//  ResourceName	- Строка - Имя или шаблон имени ресурса в зависимости от значения параметра ResourcePatternType.
//							   Пустая строка или неопределено = все ресурсы. 
//  ResourcePattern	- Строка - Способ поиска по имени ресурса:
//							   Any - поиск по имени не применяется.
//							   Literal - поиск по точному имени.
//							   Match - поиск по шаблону, допускается символ "*".
//							   Prefixed - поиск по префиксу.
//  EntryPrincipal	- Строка - Имя пользователя.
//							   Пустая строка или неопределено = все пользователи.
//							   Для пользователей, аутентифицирующихся по логину, указывается в формате "User:Sergey".
//  EntryHost		- Строка - IP-адрес клиента.
//							   Пустая строка или неопределено = все адреса.
//  EntryOperation	- Строка - All, Read, Write, Create, Delete, Describe, Alter, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite
//  EntryPermission	- Строка - Allow, Deny
// 
// Возвращаемое значение:
//  Структура - заполненная структура AclBinding
//
Функция NewAclBinding(ResourceType="Unknown", ResourceName=Неопределено, ResourcePatternType="Unknown", EntryPrincipal=Неопределено, EntryHost=Неопределено, EntryOperation="Unknown", EntryPermission="Unknown") Экспорт
	
	Если ResourceType <> "Unknown" // значение по умолчанию, требуется переопределить
	//И	 ResourceType <> "Any"
	И	 ResourceType <> "Topic"
	И	 ResourceType <> "Group"
	И	 ResourceType <> "Broker"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""ResourceType"".";
	КонецЕсли;
	
	Если ResourceName <> Неопределено Тогда
		Если ТипЗнч(ResourceName) <> Тип("Строка") Тогда
			ВызватьИсключение "Некорректное значение параметра ""ResourceName"".";
		КонецЕсли;
	КонецЕсли;
	
	Если ResourcePatternType <> "Unknown" // значение по умолчанию, требуется переопределить
	//И	 ResourcePatternType <> "Any"
	И	 ResourcePatternType <> "Literal"
	//И	 ResourcePatternType <> "Match"
	И	 ResourcePatternType <> "Prefixed"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""ResourcePattern"".";
	КонецЕсли;
	
	Если EntryPrincipal <> Неопределено Тогда
		Если ТипЗнч(EntryPrincipal) <> Тип("Строка") Тогда
			ВызватьИсключение "Некорректное значение параметра ""EntryPrincipal"".";
		КонецЕсли;
	КонецЕсли;
	
	Если EntryHost <> Неопределено Тогда
		Если ТипЗнч(EntryHost) <> Тип("Строка") Тогда
			ВызватьИсключение "Некорректное значение параметра ""EntryHost"".";
		КонецЕсли;
	КонецЕсли;
	
	Если EntryOperation <> "Unknown" // значение по умолчанию, требуется переопределить
	И	 EntryOperation <> "All"
	И	 EntryOperation <> "Read"
	И	 EntryOperation <> "Write"
	И	 EntryOperation <> "Create"
	И	 EntryOperation <> "Delete"
	И	 EntryOperation <> "Describe"
	И	 EntryOperation <> "Alter"
	И	 EntryOperation <> "ClusterAction"
	И	 EntryOperation <> "DescribeConfigs"
	И	 EntryOperation <> "AlterConfigs"
	И	 EntryOperation <> "IdempotentWrite"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""EntryOperation"".";
	КонецЕсли;
	
	Если EntryPermission <> "Unknown" // значение по умолчанию, требуется переопределить
	И	 EntryPermission <> "Allow"
	И	 EntryPermission <> "Deny"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""Permission"".";
	КонецЕсли;
	
	ResourcePattern = Новый Структура;
	ResourcePattern.Вставить("ResourceType", ResourceType);
	ResourcePattern.Вставить("Name", ?(ЗначениеЗаполнено(ResourceName), ResourceName, ?(ResourcePatternType="Literal", "*", Неопределено)));
	ResourcePattern.Вставить("PatternType", ResourcePatternType);
	
	AccessControlEntry = Новый Структура;
	AccessControlEntry.Вставить("Principal", ?(ЗначениеЗаполнено(EntryPrincipal), EntryPrincipal, "*"));
	AccessControlEntry.Вставить("Host", ?(ЗначениеЗаполнено(EntryHost), EntryHost, "*"));
	AccessControlEntry.Вставить("Operation", EntryOperation);
	AccessControlEntry.Вставить("Permission", EntryPermission);
	
	AclBinding = Новый Структура;
	AclBinding.Вставить("Pattern", ResourcePattern);
	AclBinding.Вставить("Entry", AccessControlEntry);
	
	Возврат AclBinding;
	
КонецФункции

// Формирует готовую структуру AclBindingFilter для использования в функциях AdminDescribeAcls и AdminDeleteAcls
//
// Параметры:
//  ResourceType	- Строка - Тип ресурса: Any, Topic, Group, Broker.
//  ResourceName	- Строка - Имя или шаблон имени ресурса в зависимости от значения параметра ResourcePatternType.
//							   Пустая строка или неопределено = все ресурсы. 
//  ResourcePattern	- Строка - Способ поиска по имени ресурса:
//							   Any - поиск по имени не применяется.
//							   Literal - поиск по точному имени.
//							   Match - поиск по шаблону, допускается символ "*".
//							   Prefixed - поиск по префиксу.
//  EntryPrincipal	- Строка - Имя пользователя.
//							   Пустая строка или неопределено = все пользователи.
//							   Для пользователей, аутентифицирующихся по логину, указывается в формате "User:Sergey".
//  EntryHost		- Строка - IP-адрес клиента.
//							   Пустая строка или неопределено = все адреса.
//  Operation		- Строка - Any, All, Read, Write, Create, Delete, Describe, Alter, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite
//  Permission		- Строка - Any, Allow, Deny
// 
// Возвращаемое значение:
//  Структура - заполненная структура AclBindingFilter
//
Функция NewAclBindingFilter(ResourceType="Any", ResourceName=Неопределено, ResourcePatternType="Any", EntryPrincipal=Неопределено, EntryHost=Неопределено, EntryOperation="Any", EntryPermission="Any") Экспорт
	
	Если ResourceType <> "Unknown" // значение по умолчанию, требуется переопределить
	И	 ResourceType <> "Any"
	И	 ResourceType <> "Topic"
	И	 ResourceType <> "Group"
	И	 ResourceType <> "Broker"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""ResourceType"".";
	КонецЕсли;
	
	Если ResourceName <> Неопределено Тогда
		Если ТипЗнч(ResourceName) <> Тип("Строка") Тогда
			ВызватьИсключение "Некорректное значение параметра ""ResourceName"".";
		КонецЕсли;
	КонецЕсли;
	
	Если ResourcePatternType <> "Unknown" // значение по умолчанию, требуется переопределить
	И	 ResourcePatternType <> "Any"
	И	 ResourcePatternType <> "Literal"
	И	 ResourcePatternType <> "Match"
	И	 ResourcePatternType <> "Prefixed"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""ResourcePattern"".";
	КонецЕсли;
	
	Если EntryPrincipal <> Неопределено Тогда
		Если ТипЗнч(EntryPrincipal) <> Тип("Строка") Тогда
			ВызватьИсключение "Некорректное значение параметра ""EntryPrincipal"".";
		КонецЕсли;
	КонецЕсли;
	
	Если EntryHost <> Неопределено Тогда
		Если ТипЗнч(EntryHost) <> Тип("Строка") Тогда
			ВызватьИсключение "Некорректное значение параметра ""EntryHost"".";
		КонецЕсли;
	КонецЕсли;
	
	Если EntryOperation <> "Unknown" // значение по умолчанию, требуется переопределить
	И	 EntryOperation <> "Any"
	И	 EntryOperation <> "All"
	И	 EntryOperation <> "Read"
	И	 EntryOperation <> "Write"
	И	 EntryOperation <> "Create"
	И	 EntryOperation <> "Delete"
	И	 EntryOperation <> "Describe"
	И	 EntryOperation <> "Alter"
	И	 EntryOperation <> "ClusterAction"
	И	 EntryOperation <> "DescribeConfigs"
	И	 EntryOperation <> "AlterConfigs"
	И	 EntryOperation <> "IdempotentWrite"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""EntryOperation"".";
	КонецЕсли;
	
	Если EntryPermission <> "Unknown" // значение по умолчанию, требуется переопределить
	И	 EntryPermission <> "Any"
	И	 EntryPermission <> "Allow"
	И	 EntryPermission <> "Deny"
	Тогда
		ВызватьИсключение "Некорректное значение параметра ""Permission"".";
	КонецЕсли;
	
	ResourcePatternFilter = Новый Структура;
	ResourcePatternFilter.Вставить("ResourceType", ResourceType);
	ResourcePatternFilter.Вставить("Name", ?(ResourceName = "", Неопределено, ResourceName));
	ResourcePatternFilter.Вставить("PatternType", ResourcePatternType);
	
	AccessControlEntryFilter = Новый Структура;
	AccessControlEntryFilter.Вставить("Principal", ?(EntryPrincipal = "", Неопределено, EntryPrincipal));
	AccessControlEntryFilter.Вставить("Host", ?(EntryHost = "", Неопределено, EntryHost));
	AccessControlEntryFilter.Вставить("Operation", EntryOperation);
	AccessControlEntryFilter.Вставить("Permission", EntryPermission);
	
	AclBindingFilter = Новый Структура;
	AclBindingFilter.Вставить("PatternFilter", ResourcePatternFilter);
	AclBindingFilter.Вставить("EntryFilter", AccessControlEntryFilter);
	
	Возврат AclBindingFilter;
	
КонецФункции

#КонецОбласти

#Область Отправка

Функция ProducerCreate(Name, Config, KeyType, ValueType, Знач ExpirationTimeout=Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	HttpОтвет = ProducerCreate_(Name, Config, KeyType, ValueType, ExpirationTimeout);
	
	Producer = ПрочитатьТелоОтвета(HttpОтвет);
	
	Возврат Producer;
	
КонецФункции
Функция ProducerCreate_(Name, Config, KeyType, ValueType, ExpirationTimeout)
	
	Body = Новый Структура;
	Body.Вставить("Name", Name);
	Body.Вставить("Config", Config);
	Body.Вставить("KeyType", KeyType);
	Body.Вставить("ValueType", ValueType);
	Body.Вставить("ExpirationTimeoutMs", ExpirationTimeout);
	
	HttpЗапрос = Новый HTTPЗапрос("producer");
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Body);

	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerRelease(ProducerId, Token) Экспорт
	
	// Если уже была попытка удалить экземаляр, вторую попытку не производим.
	Если ProducerId = Null Тогда
		Возврат Истина;
	КонецЕсли;
	ProducerId_ = ProducerId;
	ProducerId = Null;
	
	КодОтвета_ = КодОтвета;
	ОписаниеОшибки_ = ОписаниеОшибки;
	
	HttpОтвет = ProducerRelease_(ProducerId_, Token);
	
	// ???
	Если КодОтвета_<200 Или КодОтвета_>299 Тогда
		КодОтвета = КодОтвета_;
		ОписаниеОшибки = ОписаниеОшибки_;
	КонецЕсли;
	
	Возврат (HttpОтвет <> Неопределено);
	
КонецФункции
Функция ProducerRelease_(ProducerId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("producer/"+ProducerId+"?token="+Token);
		
	HttpОтвет = Соединение.Удалить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerGet(ProducerId) Экспорт
	
	Если Не ЗначениеЗаполнено(ProducerId) Тогда
		ВызватьИсключение "Не заполнен Id отправителя. Возможно, экземпляр отправителя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ProducerGet_(ProducerId);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ProducerGet_(ProducerId)
	
	HttpЗапрос = Новый HTTPЗапрос("producer/"+ProducerId);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция ProducerTouch(ProducerId, Token) Экспорт
	
	Если Не ЗначениеЗаполнено(ProducerId) Тогда
		ВызватьИсключение "Не заполнен Id отправителя. Возможно, экземпляр отправителя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ProducerTouch_(ProducerId, Token);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ProducerTouch_(ProducerId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("producer/"+ProducerId+"/touch?token="+Token);
		
	HttpОтвет = Соединение.Записать(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerSetSchema(ProducerId, Token, SchemaString) Экспорт
	
	Если Не ЗначениеЗаполнено(ProducerId) Тогда
		ВызватьИсключение "Не заполнен Id отправителя. Возможно, экземпляр отправителя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ProducerSetSchema_(ProducerId, Token, SchemaString);
	
	Возврат (HttpОтвет <> Неопределено);
	
КонецФункции
Функция ProducerSetSchema_(ProducerId, Token, SchemaString)
		
	HttpЗапрос = Новый HTTPЗапрос("producer/"+ProducerId+"/schemas?token="+Token);
	
	HttpЗапрос.Заголовки.Вставить("Content-Type", "text/plain; charset=utf-8");
	
	HttpЗапрос.УстановитьТелоИзСтроки(SchemaString);
	
	HttpОтвет = Соединение.Записать(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ProducerProduce(ProducerId, Token, Topic, Partition=Неопределено, Key=Неопределено, Value, Headers=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(ProducerId) Тогда
		ВызватьИсключение "Не заполнен Id отправителя. Возможно, экземпляр отправителя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ProducerProduce_(ProducerId, Token, Topic, Partition, Key, Value, Headers);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ProducerProduce_(ProducerId, Token, Topic, Partition, KeyString, ValueString, Headers)
		
	Body = Новый Структура;
	Если Headers <> Неопределено Тогда
		Body.Вставить("Headers", Headers);
	КонецЕсли;
	Если KeyString <> Неопределено Тогда
		Body.Вставить("Key", KeyString);
	КонецЕсли;
	Body.Вставить("Value", ValueString);
	
	HttpЗапрос = Новый HTTPЗапрос("producer/"+ProducerId+"/produce?token="+Token+"&topic="+Topic);
	
	Если Partition <> Неопределено Тогда
		HttpЗапрос.АдресРесурса = HttpЗапрос.АдресРесурса + "&partition="+Формат(Partition, "ЧН=0; ЧГ=0");
	КонецЕсли;
	
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Body);

	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

// Высокоуровневая процедура для отправки одного единственного сообщения.
// Не рекомендуется к использованию.
Функция ОтправитьСообщение(ИмяОперации, Конфигурация, Тема, Раздел=Неопределено, Ключ=Неопределено, Значение, Заголовки=Неопределено, ТипКлюча="String", ТипЗначения="String", СхемаКлюча=Неопределено, СхемаЗначения=Неопределено) Экспорт
	
	Producer = ProducerCreate(ИмяОперации, Конфигурация, ТипКлюча, ТипЗначения);
	
	Если Producer = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
	
	// Конструкция, гарантирующая удаление экземпляра отправителя независимо от того, чем завершится отправка.
	Попытка
		
		Если СхемаКлюча <> Неопределено И СтрНайти(Врег(ТипКлюча), "AVRO") > 0 Тогда
			ProducerSetSchema(Producer.Id, Producer.Token, СхемаКлюча);
		КонецЕсли;
		
		Если СхемаЗначения <> Неопределено И СтрНайти(Врег(ТипЗначения), "AVRO") > 0 Тогда
			ProducerSetSchema(Producer.Id, Producer.Token, СхемаЗначения);
		КонецЕсли;
		
		DeliveryResult = ProducerProduce(Producer.Id, Producer.Token, Тема, Раздел, Ключ, Значение, Заголовки);
				
		ProducerRelease(Producer.Id, Producer.Token);
		
	Исключение
				
		ProducerRelease(Producer.Id, Producer.Token);
		
		ВызватьИсключение;
		
	КонецПопытки;
	
	Возврат DeliveryResult;
	
КонецФункции

#КонецОбласти

#Область Получение

Функция ConsumerCreate(Name, Config, KeyType, ValueType, Знач ExpirationTimeout=Неопределено) Экспорт
	
	Если ExpirationTimeout = Неопределено Тогда
		ExpirationTimeout = 60000;
	КонецЕсли;
	
	HttpОтвет = ConsumerCreate_(Name, Config, KeyType, ValueType, ExpirationTimeout);
	
	Consumer = ПрочитатьТелоОтвета(HttpОтвет);
	
	Возврат Consumer;
	
КонецФункции
Функция ConsumerCreate_(Name, Config, KeyType, ValueType, ExpirationTimeout)
	
	Body = Новый Структура;
	Body.Вставить("Name", Name);
	Body.Вставить("Config", Config);
	Body.Вставить("KeyType", KeyType);
	Body.Вставить("ValueType", ValueType);
	Body.Вставить("ExpirationTimeoutMs", ExpirationTimeout);
	
	HttpЗапрос = Новый HTTPЗапрос("consumer");
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Body);

	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerRelease(ConsumerId, Token) Экспорт
	
	// Если уже была попытка удалить экземаляр, вторую попытку не производим.
	Если ConsumerId = Null Тогда
		Возврат Истина;
	КонецЕсли;
	ConsumerId_ = ConsumerId;
	ConsumerId = Null;
	
	КодОтвета_ = КодОтвета;
	ОписаниеОшибки_ = ОписаниеОшибки;
	
	HttpОтвет = ConsumerRelease_(ConsumerId_, Token);
	
	// ???
	Если КодОтвета_<200 Или КодОтвета_>299 Тогда
		КодОтвета = КодОтвета_;
		ОписаниеОшибки = ОписаниеОшибки_;
	КонецЕсли;
	
	Возврат (HttpОтвет<>Неопределено);
	
КонецФункции
Функция ConsumerRelease_(ConsumerId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId+"?token="+Token);
		
	HttpОтвет = Соединение.Удалить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGet(ConsumerId) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не заполнен Id получателя. Возможно, экземпляр получателя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ConsumerGet_(ConsumerId);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGet_(ConsumerId)
	
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция ConsumerTouch(ConsumerId, Token) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не заполнен Id получателя. Возможно, экземпляр отправителя уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ConsumerTouch_(ConsumerId, Token);
		
	Возврат ?(HttpОтвет = Неопределено, Неопределено, Истина);
	
КонецФункции
Функция ConsumerTouch_(ConsumerId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId+"/touch?token="+Token);
		
	HttpОтвет = Соединение.Записать(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerGetAssignment(ConsumerId, Token) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не задан Id получателя. Возможно, получатель уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ConsumerGetAssignment_(ConsumerId, Token);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetAssignment_(ConsumerId, Token)
	
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId+"/assignment?token="+Token);
		
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция ConsumerAssign(ConsumerId, Token, Partitions) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не задан Id получателя. Возможно, получатель уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ConsumerAssign_(ConsumerId, Token, Partitions);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerAssign_(ConsumerId, Token, Partitions)
	
	Если ТипЗнч(Partitions) = Тип("Массив") Тогда
		Body = Partitions;
	Иначе
		Body = Новый Массив;
		Если Partitions <> Неопределено Тогда
			Body.Добавить(Partitions);
		КонецЕсли;
	КонецЕсли;
		
	//Body.Вставить("Partitions", Новый Массив);
	//Для Каждого ТемаРазделСмещение Из МассивНазначение Цикл
	//	
	//	Partition = Новый Структура;
	//	Body.Partitions.Добавить(Partition);
	//	
	//	Для Каждого КЗ Из ТемаРазделСмещение Цикл
	//		Если КЗ.Ключ = "Тема" Тогда
	//			Partition.Вставить("Topic", КЗ.Значение)
	//		ИначеЕсли КЗ.Ключ = "Раздел" Тогда
	//			Partition.Вставить("Partition", КЗ.Значение);
	//		ИначеЕсли КЗ.Ключ = "Смещение" Тогда
	//			Partition.Вставить("Offset", Формат(КЗ.Значение, "ЧН=0; ЧГ=0"));
	//		КонецЕсли;
	//	КонецЦикла;
	//	Для Каждого КЗ Из ТемаРазделСмещение Цикл
	//		Если КЗ.Ключ = "Топик" Тогда
	//			Partition.Вставить("Topic", КЗ.Значение)
	//		ИначеЕсли КЗ.Ключ = "Партиция" Тогда
	//			Partition.Вставить("Partition", КЗ.Значение);
	//		ИначеЕсли КЗ.Ключ = "Офсет" Тогда
	//			Partition.Вставить("Offset", Формат(КЗ.Значение, "ЧН=0; ЧГ=0"));
	//		КонецЕсли;
	//	КонецЦикла;
	//	Для Каждого КЗ Из ТемаРазделСмещение Цикл
	//		Если КЗ.Ключ = "Topic" Тогда
	//			Partition.Вставить("Topic", КЗ.Значение)
	//		ИначеЕсли КЗ.Ключ = "Partition" Тогда
	//			Partition.Вставить("Partition", КЗ.Значение);
	//		ИначеЕсли КЗ.Ключ = "Offset" Тогда
	//			Partition.Вставить("Offset", Формат(КЗ.Значение, "ЧН=0; ЧГ=0"));
	//		КонецЕсли;
	//	КонецЦикла;
	//	
	//КонецЦикла;
		
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId+"/assignment?token="+Token);
		
	ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Body);

	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции

Функция ConsumerQueryPartitionOffsets(ConsumerId, Token, Topic, Partition, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не задан Id получателя. Возможно, получатель уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = ConsumerGetPartitionOffsets_(ConsumerId, Token, Topic, Partition, Timeout);
		
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetPartitionOffsets_(ConsumerId, Token, Topic, Partition, Timeout)
	
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId+"/offsets?token="+Token+"&topic="+Topic+"&partition="+Формат(Partition, "ЧН=0; ЧГ=0"));
	
	Если Timeout <> Неопределено Тогда
		HttpЗапрос.АдресРесурса = HttpЗапрос.АдресРесурса + "&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	КонецЕсли;
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция ConsumerConsume(ConsumerId, Token, Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не задан Id получателя. Возможно, получатель уже был удален.";
	КонецЕсли;
	
	HttpОтвет = ConsumerConsume_(ConsumerId, Token, Timeout);
	
	СоответствиеРезультат = ПрочитатьТелоОтвета(HttpОтвет, Истина);
	
	Если СоответствиеРезультат = Неопределено Или СоответствиеРезультат = Null Тогда
		Возврат СоответствиеРезультат;
	КонецЕсли;
	
	Результат = Новый Структура;
	Для Каждого КЗ Из СоответствиеРезультат Цикл
		Результат.Вставить(КЗ.Ключ, КЗ.Значение);
	КонецЦикла;
			
	Возврат Результат;
	
КонецФункции
Функция ConsumerConsume_(ConsumerId, Token, Timeout)
		
	HttpЗапрос = Новый HTTPЗапрос("consumer/"+ConsumerId+"/consume?token="+Token);
	
	ПараметрыЗапроса = "";
	Если Timeout <> Неопределено Тогда
		ПараметрыЗапроса = ПараметрыЗапроса + "&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	КонецЕсли;
	
	HttpЗапрос.АдресРесурса = HttpЗапрос.АдресРесурса + ПараметрыЗапроса;
		
	HttpОтвет = Соединение.ОтправитьДляОбработки(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

Функция ConsumerGetMetadata(ConsumerId, Token, Topic=Неопределено, Знач Timeout=Неопределено) Экспорт
	
	Если Не ЗначениеЗаполнено(ConsumerId) Тогда
		ВызватьИсключение "Не задан Id получателя. Возможно, получатель уже был удален.";
	КонецЕсли;
	
	Если Timeout = Неопределено Тогда
		Timeout = 10000;
	КонецЕсли;
	
	HttpОтвет = ConsumerGetMetadata_(ConsumerId, Token, Topic, Timeout);
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция ConsumerGetMetadata_(ConsumerId, Token, Topic, Timeout)
	
	АдресРесурса = "consumer/"+ConsumerId+"/metadata?token="+Token;
	Если Topic <> Неопределено Тогда
		АдресРесурса = АдресРесурса + "&topic="+Topic;
	КонецЕсли;
	АдресРесурса = АдресРесурса + "&timeout="+Формат(Timeout, "ЧН=0; ЧГ=0");
	
	HttpЗапрос = Новый HTTPЗапрос(АдресРесурса);
	
	HttpОтвет = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(HttpОтвет);
	
КонецФункции	

// Высокоуровневая процедура получения среза последних сообщений (для тем с обрезкой "compact").
Функция ПолучитьСрезПоследнихСообщений(ИмяОперации, Конфигурация, Тема, Раздел=Неопределено, ТипКлюча, ТипЗначения) Экспорт
	
	Конфигурация2 = Новый Соответствие;
	Для Каждого КЗ Из Конфигурация Цикл
		Конфигурация2.Вставить(КЗ.Ключ, КЗ.Значение);
	КонецЦикла;
	Конфигурация2.Вставить("enable.partition.eof", XMLСтрока(Истина));
	
	Consumer = ConsumerCreate(ИмяОперации, Конфигурация2, ТипКлюча, ТипЗначения);
	Если Consumer = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
					
	Попытка
		Результат = ПолучитьСрезПоследнихСообщений2(Consumer, Тема, Раздел);
		ConsumerRelease(Consumer.Id, Consumer.Token);
	Исключение
		ConsumerRelease(Consumer.Id, Consumer.Token);
		ВызватьИсключение;
	КонецПопытки;
			
	Возврат Результат;
	
КонецФункции
Функция ПолучитьСрезПоследнихСообщений2(Consumer, Тема, Раздел=Неопределено) Экспорт
	
	Если Раздел = Неопределено Тогда
	
		Md = ConsumerGetMetadata(Consumer.Id, Consumer.Token, Тема);
		Если Md = Неопределено Тогда
			Возврат Неопределено;
		КонецЕсли;
		
		TopicMd = Md.Topics[0];
		
		Если TopicMd.Partitions.Количество() = 0 Тогда
			Возврат Новый Соответствие;
		КонецЕсли;
		
	Иначе
		
		TopicMd = Новый Структура;
		TopicMd.Вставить("Topic", Тема);
		TopicMd.Вставить("Partitions", Новый Массив);
		
		PartitionMd = Новый Структура;
		PartitionMd.Вставить("Partition", Раздел);
		
		TopicMd.Partitions.Добавить(PartitionMd);
		
	КонецЕсли;
	
	МассивНазначение = Новый Массив;
	Для Каждого PartitionMd Из TopicMd.Partitions Цикл
		
		PartitionOffsets = ConsumerQueryPartitionOffsets(Consumer.Id, Consumer.Token, TopicMd.Topic, PartitionMd.Partition);
		Если PartitionOffsets = Неопределено Тогда
			Возврат Неопределено;
		КонецЕсли;
		
		МассивНазначение.Добавить(Новый Структура("Topic, Partition, Offset", TopicMd.Topic, PartitionMd.Partition, PartitionOffsets.Low));
		
	КонецЦикла;
			
	МассивНазначение = ConsumerAssign(Consumer.Id, Consumer.Token, МассивНазначение);
	Если МассивНазначение = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
	
	РазделовОсталось = МассивНазначение.Количество();
	
	Результат = Новый Соответствие;
	
	Пока Истина Цикл
		
		Message = ConsumerConsume(Consumer.Id, Consumer.Token, 60000);
		Если Message = Неопределено Тогда
			Возврат Неопределено;
		КонецЕсли;
		
		Если Message = Null Тогда
			
			Прервать;
		
		ИначеЕсли Message.IsPartitionEOF Тогда
			
			РазделовОсталось = РазделовОсталось - 1;
			Если РазделовОсталось = 0 Тогда
				Прервать;
			КонецЕсли;
			
		Иначе
		
			Результат.Вставить(Message.Key, Message);
			
		КонецЕсли;
		
	КонецЦикла;
		
	Возврат Результат;
	
КонецФункции

// Высокоуровневая процедура получения смещений темы.
Функция ПолучитьСмещенияТемы(ИмяОперации, Конфигурация, Тема, Знач Таймаут=Неопределено) Экспорт
	
	Если Таймаут = Неопределено Тогда
		Таймаут = 10000;
	КонецЕсли;
	
	Consumer = ConsumerCreate(ИмяОперации, Конфигурация, "Ignore", "Ignore");
	Если Consumer = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
					
	Попытка
		Результат = ПолучитьСмещенияТемы2(Consumer, Тема, Таймаут);
		ConsumerRelease(Consumer.Id, Consumer.Token);
	Исключение
		ConsumerRelease(Consumer.Id, Consumer.Token);
		ВызватьИсключение;
	КонецПопытки;
				
	Возврат Результат;
	
КонецФункции
Функция ПолучитьСмещенияТемы2(Consumer, Тема, Знач Таймаут=Неопределено) Экспорт

	Если Таймаут = Неопределено Тогда
		Таймаут = 10000;
	КонецЕсли;
	
	Результат = Новый Массив;
	
	Если СтрЗаканчиваетсяНа(Тема, "*") Тогда

		Md = ConsumerGetMetadata(Consumer.Id, Consumer.Token);
		Если Md = Неопределено Тогда
			Возврат Неопределено;
		КонецЕсли;
		
		ТемаПрефикс = Лев(Тема, СтрДлина(Тема)-1);
		
		Список = Новый СписокЗначений;
		
		Для Каждого TopicMd Из Md.Topics Цикл
			
			Если СтрНачинаетсяС(TopicMd.Topic, ТемаПрефикс) Тогда
				Список.Добавить(TopicMd, TopicMd.Topic);
			КонецЕсли;
			
		КонецЦикла;
		
		Список.СортироватьПоПредставлению();
		
		MdTopics = Список.ВыгрузитьЗначения();
		
	Иначе
		
		Md = ConsumerGetMetadata(Consumer.Id, Consumer.Token, Тема);
		Если Md = Неопределено Тогда
			Возврат Неопределено;
		КонецЕсли;
		
		MdTopics = Md.Topics;
		
	КонецЕсли;
			
	Для Каждого TopicMd Из MdTopics Цикл
	
		Для Каждого PartitionMd Из TopicMd.Partitions Цикл
			
			PartitionOffsets = ConsumerQueryPartitionOffsets(Consumer.Id, Consumer.Token, TopicMd.Topic, PartitionMd.Partition, Таймаут);
			Если PartitionOffsets = Неопределено Тогда
				Возврат Неопределено;
			КонецЕсли;
			
			Результат.Добавить(PartitionOffsets);
			
		КонецЦикла;
	
	КонецЦикла;
		
	Возврат Результат;
	
КонецФункции

// Высокоуровневая процедура получения метаданных темы.
Функция ПолучитьМетаданныеТемы(ИмяОперации, Конфигурация, Тема) Экспорт
	
	Consumer = ConsumerCreate(ИмяОперации, Конфигурация, "Ignore", "Ignore");
	Если Consumer = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
					
	Попытка
		Результат = ПолучитьМетаданныеТемы2(Consumer, Тема);
		ConsumerRelease(Consumer.Id, Consumer.Token);
	Исключение
		ConsumerRelease(Consumer.Id, Consumer.Token);
		ВызватьИсключение;
	КонецПопытки;
			
	Возврат Результат;
	
КонецФункции
Функция ПолучитьМетаданныеТемы2(Consumer, Тема) Экспорт
	
	Md = ConsumerGetMetadata(Consumer.Id, Consumer.Token, Тема);
	
	Если Md = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
	
	TopicMd = Md.Topics[0];
		
	Возврат TopicMd;
	
КонецФункции

Функция СпецСмещениеНачало()
	Возврат -2;
КонецФункции
Функция СпецСмещениеКонец()
	Возврат -1;
КонецФункции
Функция СпецСмещениеНеУстановлено()
	Возврат -1001;
КонецФункции
Функция СпецСмещениеЗаписано()
	Возврат -1000;
КонецФункции

#КонецОбласти

#Область Прокси

Функция GetProxyInfo() Экспорт
		
	HttpОтвет = GetProxyInfo_();
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция GetProxyInfo_()
	
	HttpЗапрос = Новый HTTPЗапрос("");
		
	Ответ = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(Ответ);
	
КонецФункции	

Функция GetProxyHealth() Экспорт
		
	HttpОтвет = GetProxyHealth_();
	
	Возврат ПрочитатьТелоОтвета(HttpОтвет);
	
КонецФункции
Функция GetProxyHealth_()
	
	HttpЗапрос = Новый HTTPЗапрос("health");
		
	Ответ = Соединение.Получить(HttpЗапрос);
	
	Возврат ПроверитьОтвет(Ответ);
	
КонецФункции	

#КонецОбласти

#Область ВспомогательныеФункции

Функция ПрочитатьТелоОтвета(HttpОтвет, КакСоответствие=Ложь)
	
	Если HttpОтвет = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
	
	Если HttpОтвет.КодСостояния = 204 Тогда
		Возврат NULL;
	КонецЕсли;
		
	ContentType = HttpОтвет.Заголовки.Получить("Content-Type");
	Если ContentType = Неопределено Тогда
		
		ContentLength = HttpОтвет.Заголовки.Получить("Content-Length");
		Если ContentLength = "0" Тогда
			Возврат NULL;
		Иначе
			ВызватьИсключение "Неожиданное состояние.";
		КонецЕсли;
		
	ИначеЕсли СтрНачинаетсяС(ContentType, "application/json") Тогда
		
		Поток = HttpОтвет.ПолучитьТелоКакПоток();
		
		ЧтениеJSON = Новый ЧтениеJSON;
		ЧтениеJSON.ОткрытьПоток(Поток);
		Результат = ПрочитатьJSON(ЧтениеJSON, КакСоответствие);
		ЧтениеJSON.Закрыть();
		
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

Процедура ЗаписатьJSONвHttpЗапрос(HttpЗапрос, Значение)
	
	HttpЗапрос.Заголовки.Вставить("Content-Type", "application/json; charset=utf-8");
	
	Поток = HttpЗапрос.ПолучитьТелоКакПоток();
	
	ЗаписьJSON = Новый ЗаписьJSON;
	ЗаписьJSON.ОткрытьПоток(Поток, , , ПараметрыЗаписиJSON);
	ЗаписатьJSON(ЗаписьJSON, Значение);
	ЗаписьJSON.Закрыть();
	
	//Поток.Закрыть();
	
КонецПроцедуры

Функция ЗначениеКакJSON(Значение)
		
	ЗаписьJSON = Новый ЗаписьJSON;
	ЗаписьJSON.УстановитьСтроку(ПараметрыЗаписиJSON);
	ЗаписатьJSON(ЗаписьJSON, Значение);
	Результат = ЗаписьJSON.Закрыть();
	
	Возврат Результат;
	
КонецФункции

Функция ПроверитьОтвет(HttpОтвет)
	
	КодОтвета = HttpОтвет.КодСостояния;
	
	Если HttpОтвет.КодСостояния<200 Или HttpОтвет.КодСостояния>299 Тогда
		ОписаниеОшибки = HttpОтвет.ПолучитьТелоКакСтроку();
		Возврат Неопределено;
	КонецЕсли;
	
	ОписаниеОшибки = Неопределено;
	Возврат HttpОтвет;
	
КонецФункции

#КонецОбласти
