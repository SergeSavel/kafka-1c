﻿// Copyright 2023 Савельев Сергей Владимирович
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

&НаКлиенте
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

	Если Элементы.ЗагрузкаПозиции.ТекущаяСтрока = Неопределено
	И Объект.ОбменНаправление = ПредопределенноеЗначение("Перечисление.КафкаНаправленияОбмена.Загрузка") И Не ПустаяСтрока(Объект.Тема)
	И ПараметрыФоновогоЗадания=Неопределено Тогда
		ОбновитьПозицииЗагрузки();
	КонецЕсли;
	
КонецПроцедуры

&НаКлиенте
Процедура КомандаОбновитьПозицииЗагрузки(Команда)

	ОбновитьПозицииЗагрузки();
	
КонецПроцедуры

&НаКлиенте
Процедура ОбновитьПозицииЗагрузки()
	
	ЗаполнитьТекущиеПозицииЧтения();
	
	ПараметрыФоновогоЗадания = ЗапуститьПолучениеСмещенийТемыВФоне(Объект.Кластер, Объект.Тема, ЭтаФорма.УникальныйИдентификатор);
	Если ПараметрыФоновогоЗадания <> Неопределено Тогда
		ПодключитьОбработчикОжидания("ОпросФоновогоЗадания", 2, Ложь);
	КонецЕсли;
	
КонецПроцедуры

&НаСервере
Процедура ЗагрузкаПозицииИнициализировать()
	
	ЗагрузкаПозиции.Очистить();
	ЗагрузкаПозицияТекущая   = "<не применимо>";
	ЗагрузкаПозицияНачальная = "<не получено>";
	ЗагрузкаПозицияКонечная  = "<не получено>";
	ЗагрузкаОтставание		 = "<не получено>";
	
КонецПроцедуры

&НаСервере
Процедура ЗаполнитьТекущиеПозицииЧтения()
	
	ЗагрузкаПозицииИнициализировать();
					
	_ЗагрузкаПозицияТекущая   = 0;
	
	_Тема = КафкаСервер.Тема(Объект.Тема);
	_Тема = СтрЗаменить(_Тема, "*", "%");
	
	Запрос = Новый Запрос(
	"ВЫБРАТЬ
	|	Позиции.Тема КАК Тема,
	|	Позиции.Раздел КАК Раздел,
	|	Позиции.ДатаУстановки КАК ДатаУстановки,
	|	Позиции.Позиция КАК Позиция
	|ИЗ
	|	РегистрСведений.КафкаПозиции.СрезПоследних(
	|			,
	|			Кластер = &Кластер
	|				И Тема ПОДОБНО &Тема) КАК Позиции
	|
	|УПОРЯДОЧИТЬ ПО
	|	Тема,
	|	Раздел");
	Запрос.УстановитьПараметр("Кластер", Объект.Кластер);	
	Запрос.УстановитьПараметр("Тема", _Тема);	
	ТЗ = Запрос.Выполнить().Выгрузить();
	
	СтруктураПоиск = Новый Структура("Раздел");
	
	Для Каждого с Из ТЗ Цикл
		сс = ЗагрузкаПозиции.Добавить();
		сс.Тема = с.Тема;
		сс.Раздел = с.Раздел;
		сс.Текущая = Формат(с.Позиция, "ЧН=0");
		сс.ДатаФиксации = с.ДатаУстановки;
		сс.Начальная = "<не получено>";
		сс.Конечная = "<не получено>";
		сс.Отставание = "<не получено>";
		_ЗагрузкаПозицияТекущая = _ЗагрузкаПозицияТекущая + с.Позиция;
	КонецЦикла;
	
	ЗагрузкаПозицияТекущая = Формат(_ЗагрузкаПозицияТекущая, "ЧН=0");
			
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
	_ЗагрузкаОтставание  = 0;
	
	ИндексТемы = Новый Соответствие;
	Для Каждого С Из ЗагрузкаПозиции Цикл
		ИндексРазделы = ИндексТемы.Получить(С.Тема);
		Если ИндексРазделы = Неопределено Тогда
			ИндексРазделы = Новый Соответствие;
			ИндексТемы.Вставить(С.Тема, ИндексРазделы);
		КонецЕсли;
		ИндексРазделы.Вставить(С.Раздел, С);
	КонецЦикла;
	
	СтруктураПоиск = Новый Структура("Тема, Раздел");
	Для Каждого PartitionOffsets Из СмещенияТемы Цикл
				
		ИндексРазделы = ИндексТемы.Получить(PartitionOffsets.Topic);
		
		Если ИндексРазделы = Неопределено Тогда
			ИндексРазделы = Новый Соответствие;
			ИндексТемы.Вставить(PartitionOffsets.Topic, ИндексРазделы);
		КонецЕсли;
		
		СтрокаРаздел = ИндексРазделы.Получить(PartitionOffsets.Partition);
		
		Если СтрокаРаздел = Неопределено Тогда
			
			СтрокаРаздел = ЗагрузкаПозиции.Добавить();
			СтрокаРаздел.Тема = PartitionOffsets.Topic;
			СтрокаРаздел.Раздел = PartitionOffsets.Partition;
			СтрокаРаздел.Текущая = ?(ЗагрузкаПозицияТекущая="<не применимо>", ЗагрузкаПозицияТекущая, "0");
			
			ИндексРазделы.Вставить(PartitionOffsets.Partition, СтрокаРаздел);
			
		КонецЕсли;
				
		СтрокаРаздел.Начальная = Формат(PartitionOffsets.Low, "ЧН=0");
		СтрокаРаздел.Конечная = Формат(PartitionOffsets.High, "ЧН=0");
		
		_ЗагрузкаПозицияНачальная = _ЗагрузкаПозицияНачальная + PartitionOffsets.Low;
		_ЗагрузкаПозицияКонечная  = _ЗагрузкаПозицияКонечная  + PartitionOffsets.High;
		
	КонецЦикла;
	
	ЗагрузкаПозицияНачальная = _ЗагрузкаПозицияНачальная;
	ЗагрузкаПозицияКонечная  = _ЗагрузкаПозицияКонечная;
	
	Для Каждого СтрокаРаздел Из ЗагрузкаПозиции Цикл
		Если СтрокаРаздел.Текущая = "<не применимо>" Тогда
			СтрокаРаздел.Отставание = СтрокаРаздел.Текущая;
			Продолжить;
		КонецЕсли;
		Если СтрокаРаздел.Конечная = "<не получено>" Тогда
			СтрокаРаздел.Отставание = СтрокаРаздел.Конечная;
			Продолжить;
		КонецЕсли;
		ОтставаниеЧисло = Число(СтрокаРаздел.Конечная) - Макс(Число(СтрокаРаздел.Начальная), Число(СтрокаРаздел.Текущая));
		СтрокаРаздел.Отставание = Формат(ОтставаниеЧисло, "ЧН=0");
		_ЗагрузкаОтставание = _ЗагрузкаОтставание + ОтставаниеЧисло;
	КонецЦикла;
	ЗагрузкаОтставание = _ЗагрузкаОтставание;
	
	ЗагрузкаПозиции.Сортировать("Тема, Раздел");
	
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
	Иначе
		РеальнаяТема = "";
	КонецЕсли;
	
	Если ТекущийОбъект <> Неопределено Тогда
		ОбменРасписание = ТекущийОбъект.ОбменРасписание.Получить();
	КонецЕсли;
	Если ОбменРасписание = Неопределено Тогда
		ОбменРасписание = Справочники.КафкаШины.ОбменРасписаниеПоУмолчанию();
	КонецЕсли;
	Элементы.ОбменРасписание.Заголовок = Строка(ОбменРасписание);
	
	Если ОбменРасписаниеАдрес <> "" Тогда
		УдалитьИзВременногоХранилища(ОбменРасписаниеАдрес);
	КонецЕсли;
	ОбменРасписаниеАдрес = ПоместитьВоВременноеХранилище(ОбменРасписание, УникальныйИдентификатор);
	
	ЗагрузкаПозицииИнициализировать();
	
	Элементы.ФормаВыполнитьОбмен.Видимость =
			ПравоДоступа("Редактирование", Метаданные.Справочники.КафкаШины);
			
	ОбновитьВидимостьДоступность(ЭтаФорма);
			
КонецПроцедуры

&НаКлиентеНаСервереБезКонтекста
Процедура ОбновитьВидимостьДоступность(Форма)
	
	Форма.Элементы.СтраницаПрием.Видимость = (Форма.Объект.ОбменНаправление = ПредопределенноеЗначение("Перечисление.КафкаНаправленияОбмена.Загрузка"));
	Форма.Элементы.СтраницаОтправка.Видимость = (Форма.Объект.ОбменНаправление = ПредопределенноеЗначение("Перечисление.КафкаНаправленияОбмена.Выгрузка"));
	
	Форма.Элементы.ОбменДлительностьСеанса.АвтоОтметкаНезаполненного = Форма.Объект.ПриемВРеальномВремени;
	
	Форма.Элементы.ЗагрузкаПозицииТема.Видимость = СтрЗаканчиваетсяНа(Форма.Объект.Тема, "*");
	
КонецПроцедуры

&НаКлиенте
Процедура ОбменНаправлениеПриИзменении(Элемент)
	
	ОбновитьВидимостьДоступность(ЭтаФорма);
	
КонецПроцедуры

&НаКлиенте
Процедура ТемаПриИзменении(Элемент)
	
	ОбновитьВидимостьДоступность(ЭтаФорма);
	
КонецПроцедуры

&НаКлиенте
Процедура ПриемВРеальномВремениПриИзменении(Элемент)
	
	ОбновитьВидимостьДоступность(ЭтаФорма);
	
КонецПроцедуры

&НаКлиенте
Процедура КомандаВыполнитьОбмен(Команда)
	
	ВыполнитьОбмен();
	
КонецПроцедуры
&НаКлиенте
Процедура ВыполнитьОбмен()
	
	Если Объект.Ссылка.Пустая() Или Модифицированность Тогда
		Сообщение = Новый СообщениеПользователю;
		Сообщение.УстановитьДанные(Объект);
		Сообщение.Текст = "Перед ручным запуском обмена объект должен быть записан.";
		Сообщение.Сообщить();
		Возврат;
	КонецЕсли;
	
	ВыполнитьОбменНаСервере(Объект.Ссылка);
	
КонецПроцедуры
&НаСервереБезКонтекста
Процедура ВыполнитьОбменНаСервере(ШинаСсылка)
	
	Справочники.КафкаШины.ВыполнитьОбмен(ШинаСсылка);
	
КонецПроцедуры
