﻿&НаКлиенте
Перем ПараметрыФоновогоЗадания;

#Область СобытияФормы

&НаСервере
Процедура ПриЧтенииНаСервере(ТекущийОбъект)
	
	ПриЧтенииСозданииНаСервере(ТекущийОбъект);
	
КонецПроцедуры

&НаСервере
Процедура ПриСозданииНаСервере(Отказ, СтандартнаяОбработка)

	Если Объект.Ссылка.Пустая() Тогда
		ПриЧтенииСозданииНаСервере();
	КонецЕсли;
			
КонецПроцедуры

&НаКлиенте
Процедура ПриОткрытии(Отказ)
		
	ОбновитьВидимостьДоступность();
	
КонецПроцедуры

&НаСервере
Процедура ПередЗаписьюНаСервере(Отказ, ТекущийОбъект, ПараметрыЗаписи)

	ОбменРасписание = ПолучитьИзВременногоХранилища(ОбменРасписаниеАдрес);
	ТекущийОбъект.ОбменРасписание = Новый ХранилищеЗначения(ОбменРасписание);
	
КонецПроцедуры

&НаСервере
Процедура ПослеЗаписиНаСервере(ТекущийОбъект, ПараметрыЗаписи)

	ПриЧтенииСозданииНаСервере(ТекущийОбъект);

КонецПроцедуры

#КонецОбласти

#Область ОбновлениеСмещенийТемы

&НаКлиенте
Процедура ЗагрузкаПозицииПриАктивизацииСтроки(Элемент)

	Если Объект.Вид="Источник" И Не ПустаяСтрока(Объект.Тема) И ПараметрыФоновогоЗадания=Неопределено Тогда
		ЗапуститьОбновлениеСмещенийТемы();
	КонецЕсли;
	
КонецПроцедуры

&НаКлиенте
Процедура ЗапуститьОбновлениеСмещенийТемы()
	
	ПараметрыФоновогоЗадания = ЗапуститьПолучениеСмещенийТемыВФоне(Объект.Кластер, Объект.Тема, ЭтаФорма.УникальныйИдентификатор);
	Если ПараметрыФоновогоЗадания <> Неопределено Тогда
		ПодключитьОбработчикОжидания("ОпросФоновогоЗадания", 2, Ложь);
	КонецЕсли;
	
КонецПроцедуры
&НаСервереБезКонтекста
Функция ЗапуститьПолучениеСмещенийТемыВФоне(Кластер, Знач Тема, ФормаУникальныйИдентификатор)
	
	Тема = КафкаСервер.Тема(Тема);
	АдресРезультата = ПоместитьВоВременноеХранилище(Неопределено, ФормаУникальныйИдентификатор);
	
	ПараметрыФЗ = Новый Массив;
	ПараметрыФЗ.Добавить(Кластер);
	ПараметрыФЗ.Добавить(Тема);
	ПараметрыФЗ.Добавить(АдресРезультата);
	
	ФЗ = ФоновыеЗадания.Выполнить("КафкаСервер.ПолучитьСмещенияТемыВФоне", ПараметрыФЗ, ,
			"Кафка: Запрос информации темы """+Тема+"""");
			
	Возврат Новый ФиксированнаяСтруктура("УникальныйИдентификатор, АдресРезультата", ФЗ.УникальныйИдентификатор, АдресРезультата);
			
КонецФункции

&НаКлиенте
Процедура ОпросФоновогоЗадания()
	
	РезультатФЗ = ОпросФоновогоЗаданияНаСервере(ПараметрыФоновогоЗадания);
	Если РезультатФЗ = Неопределено Тогда
		Возврат;
	КонецЕсли;
	
	ОтключитьОбработчикОжидания("ОпросФоновогоЗадания");
	
	Если ТипЗнч(РезультатФЗ) = Тип("Строка") Тогда
		Сообщить(РезультатФЗ);
	Иначе
		Если РезультатФЗ.Успех Тогда
			ОбновитьСмещенияТемы(РезультатФЗ.Результат);
		Иначе
			Сообщить(РезультатФЗ.ОписаниеОшибки);
		КонецЕсли;
	КонецЕсли;
	
КонецПроцедуры
&НаСервереБезКонтекста
Функция ОпросФоновогоЗаданияНаСервере(ПараметрыФЗ)
	
	ФЗ = ФоновыеЗадания.НайтиПоУникальномуИдентификатору(ПараметрыФЗ.УникальныйИдентификатор);
	Если ФЗ = Неопределено Тогда
		Возврат "Не найдено фоновое задание опроса темы.";
	КонецЕсли;
	
	Если ФЗ.Состояние = СостояниеФоновогоЗадания.Активно Тогда
		Возврат Неопределено;
	ИначеЕсли ФЗ.Состояние = СостояниеФоновогоЗадания.Завершено Тогда
		Результат = ПолучитьИзВременногоХранилища(ПараметрыФЗ.АдресРезультата);
		УдалитьИзВременногоХранилища(ПараметрыФЗ.АдресРезультата);
		Если Результат = Неопределено Тогда
			Возврат "Фоновое задание опроса темы возвратило пустой результат";
		КонецЕсли;
		Возврат Результат;
	ИначеЕсли ФЗ.Состояние = СостояниеФоновогоЗадания.ЗавершеноАварийно Тогда
		Возврат "Фоновое задание опроса темы завершено аварийно."
			+ ?(ФЗ.ИнформацияОбОшибке=Неопределено, "", Символы.ПС+КафкаКлиентСервер.КорневаяИнформацияОбОшибке(ФЗ.ИнформацияОбОшибке).Описание);
	Иначе
		Возврат "Фоновое задание опроса темы отменено.";
	КонецЕсли;
	
КонецФункции

&НаКлиенте
Процедура ОбновитьСмещенияТемы(СмещенияТемы)
	
	_ЗагрузкаПозицияНачальная = 0;
	_ЗагрузкаПозицияКонечная  = 0;
	
	ТекушаяПозицияНеПрименима = (ТипЗнч(ЗагрузкаПозицияТекущая)=Тип("Строка"));
	
	СтруктураПоиск = Новый Структура("Раздел");
	Для Каждого PartitionOffsets Из СмещенияТемы Цикл
		
		СтруктураПоиск.Раздел = PartitionOffsets.Partition;
		м = ЗагрузкаПозиции.НайтиСтроки(СтруктураПоиск);
		Если м.Количество() = 0 Тогда
			СтрокаПартиция = ЗагрузкаПозиции.Добавить();
			СтрокаПартиция.Раздел = PartitionOffsets.Partition;
			СтрокаПартиция.Текущая = ?(ЗагрузкаПозицияТекущая="<не применимо>", ЗагрузкаПозицияТекущая, "0");
		Иначе
			СтрокаПартиция = м[0];
		КонецЕсли;
		
		СтрокаПартиция.Начальная = Формат(PartitionOffsets.Low, "ЧН=0");
		СтрокаПартиция.Конечная = Формат(PartitionOffsets.High, "ЧН=0");
		
		_ЗагрузкаПозицияНачальная = _ЗагрузкаПозицияНачальная + PartitionOffsets.Low;
		_ЗагрузкаПозицияКонечная  = _ЗагрузкаПозицияКонечная  + PartitionOffsets.High;
	КонецЦикла;
	
	ЗагрузкаПозицияНачальная = _ЗагрузкаПозицияНачальная;
	ЗагрузкаПозицияКонечная  = _ЗагрузкаПозицияКонечная;
	
	ЗагрузкаПозиции.Сортировать("Раздел");
	
КонецПроцедуры

#КонецОбласти

#Область РасписаниеОбмена

&НаКлиенте
Процедура ОбменРасписаниеНажатие(Элемент)
	
	ОбменРасписаниеПоказатьДиалог();
	
КонецПроцедуры

&НаКлиенте
Процедура ОбменРасписаниеПоказатьДиалог()
	
	Расписание = ПолучитьИзВременногоХранилища(ОбменРасписаниеАдрес);
	
	Диалог = Новый ДиалогРасписанияРегламентногоЗадания(Расписание); 
	
	ОписаниеОповещения = Новый ОписаниеОповещения("ОбменРасписаниеПриЗакрытииДиалога", ЭтаФорма);
	Диалог.Показать(ОписаниеОповещения);
	
КонецПроцедуры
&НаКлиенте
Процедура ОбменРасписаниеПриЗакрытииДиалога(Расписание, ДополнительныеПараметры) Экспорт
	
	Если Расписание = Неопределено Тогда
		Возврат;
	КонецЕсли;
	
	ОбменРасписаниеАдрес = ПоместитьВоВременноеХранилище(Расписание, УникальныйИдентификатор);
	
	Элементы.ОбменРасписание.Заголовок = Строка(Расписание);
	
	Модифицированность = Истина;
	
КонецПроцедуры

#КонецОбласти

&НаСервере
Процедура ПриЧтенииСозданииНаСервере(ТекущийОбъект=Неопределено)
	
	Если Не ПустаяСтрока(Объект.Тема) Тогда
		РеальнаяТема = КафкаСервер.Тема(Объект.Тема);
		
		Если Объект.Вид = "Источник" Тогда
			ЗаполнитьТекущиеПозицииЧтения();
		КонецЕсли;
		
	Иначе
		РеальнаяТема = "";
	КонецЕсли;
	
	Если ТекущийОбъект <> Неопределено Тогда
		ОбменРасписание = ТекущийОбъект.ОбменРасписание.Получить();
	КонецЕсли;
	Если ОбменРасписание = Неопределено Тогда
		ОбменРасписание = Справочники.КафкаШиныОбмена.ОбменРасписаниеПоУмолчанию();
	КонецЕсли;
	Элементы.ОбменРасписание.Заголовок = Строка(ОбменРасписание);
	
	Если ОбменРасписаниеАдрес <> "" Тогда
		УдалитьИзВременногоХранилища(ОбменРасписаниеАдрес);
	КонецЕсли;
	ОбменРасписаниеАдрес = ПоместитьВоВременноеХранилище(ОбменРасписание, УникальныйИдентификатор);
	
КонецПроцедуры

&НаСервере
Процедура ЗаполнитьТекущиеПозицииЧтения()
	
	ЗагрузкаПозиции.Очистить();
	ЗагрузкаПозицияТекущая   = "<не применимо>";
	ЗагрузкаПозицияНачальная = "<не получено>";
	ЗагрузкаПозицияКонечная  = "<не получено>";
				
	_ЗагрузкаПозицияТекущая   = 0;
	
	Запрос = Новый Запрос(
	"ВЫБРАТЬ
	|	Позиции.Раздел КАК Раздел,
	|	Позиции.ДатаУстановки,
	|	Позиции.Позиция
	|ИЗ
	|	РегистрСведений.КафкаПозиции.СрезПоследних(, Тема = &Тема) КАК Позиции
	|
	|УПОРЯДОЧИТЬ ПО
	|	Раздел");
	Запрос.УстановитьПараметр("Тема", КафкаСервер.Тема(Объект.Тема));	
	ТЗ = Запрос.Выполнить().Выгрузить();
	
	СтруктураПоиск = Новый Структура("Раздел");
	
	Для Каждого с Из ТЗ Цикл
		сс = ЗагрузкаПозиции.Добавить();
		сс.Раздел = с.Раздел;
		сс.Текущая = Формат(с.Позиция, "ЧН=0");
		сс.ДатаФиксации = с.ДатаУстановки;
		сс.Начальная = "<не получено>";
		сс.Конечная  = "<не получено>";
		_ЗагрузкаПозицияТекущая = _ЗагрузкаПозицияТекущая + с.Позиция;
	КонецЦикла;
	
	ЗагрузкаПозицияТекущая = Формат(_ЗагрузкаПозицияТекущая, "ЧН=0");
			
КонецПроцедуры

&НаКлиенте
Процедура ОбновитьВидимостьДоступность()
	
	Элементы.ГруппаПрием.Видимость = (Объект.Вид="Источник");
	Элементы.ГруппаОтправка.Видимость = (Объект.Вид="Приемник");
	
	Элементы.ПриемДлительность.АвтоОтметкаНезаполненного = Объект.ПриемВРеальномВремени;
	
КонецПроцедуры

&НаКлиенте
Процедура ВидПриИзменении(Элемент)
	
	ОбновитьВидимостьДоступность();
	
КонецПроцедуры

&НаКлиенте
Процедура ПриемВРеальномВремениПриИзменении(Элемент)
	
	ОбновитьВидимостьДоступность();
	
КонецПроцедуры
