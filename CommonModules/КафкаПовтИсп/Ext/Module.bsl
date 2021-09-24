﻿
Функция ИндексМетаданных() Экспорт
	
	Результат = Новый Соответствие;
		
	Для Каждого Элемент Из Метаданные.Константы Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.Справочники Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.Последовательности Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.Документы Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.ПланыВидовХарактеристик Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.ПланыСчетов Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.ПланыВидовРасчета Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.РегистрыСведений Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.РегистрыНакопления Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.РегистрыБухгалтерии Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.РегистрыРасчета Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.БизнесПроцессы Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Для Каждого Элемент Из Метаданные.Задачи Цикл
		Результат.Вставить(Элемент.ПолноеИмя(), Элемент);
	КонецЦикла;
	
	Возврат Новый ФиксированноеСоответствие(Результат);
	
КонецФункции

Функция ОписаниеТаблицы(Таблица) Экспорт
	
	МетаданныеТаблицы = Метаданные.НайтиПоПолномуИмени(Таблица);
	//МетаданныеТаблицы = КафкаПовтИсп.ИндексМетаданных().Получить(Таблица);
	Если МетаданныеТаблицы = Неопределено Тогда
		Возврат Неопределено;
	КонецЕсли;
	
	Результат = Новый Структура;
	Результат.Вставить("ПолноеИмя",		МетаданныеТаблицы.ПолноеИмя());
	Результат.Вставить("Имя",			МетаданныеТаблицы.Имя);
	Результат.Вставить("Вид",			Лев(Результат.ПолноеИмя, Найти(Результат.ПолноеИмя, ".")-1));
	Результат.Вставить("Представление", МетаданныеТаблицы.Представление());
	
	Результат.Вставить("ЭтоКонстанта"		   , (Результат.Вид="Константа"));
	Результат.Вставить("ЭтоСправочник"	  	   , (Результат.Вид="Справочник"));
	Результат.Вставить("ЭтоДокумент"	  	   , (Результат.Вид="Документ"));
	Результат.Вставить("ЭтоПоследовательность" , (Результат.Вид="Последовательность"));
	Результат.Вставить("ЭтоРегистрСведений"	   , (Результат.Вид="РегистрСведений"));
	Результат.Вставить("ЭтоРегистрНакопления"  , (Результат.Вид="РегистрНакопления"));
	Результат.Вставить("ЭтоРегистрБухгалтерии" , (Результат.Вид="РегистрБухгалтерии"));
	Результат.Вставить("ЭтоРегистрРасчета"	   , (Результат.Вид="РегистрРасчета"));
	Результат.Вставить("ЭтоБизнесПроцесс"	   , (Результат.Вид="БизнесПроцесс"));
	
	Результат.Вставить("ЭтоРегистр", (Результат.ЭтоРегистрСведений Или Результат.ЭтоРегистрНакопления Или Результат.ЭтоРегистрБухгалтерии Или Результат.ЭтоРегистрРасчета));
	
	Результат.Вставить("ЭтоНезависимыйРегистр", Результат.ЭтоРегистрСведений И МетаданныеТаблицы.РежимЗаписи=Метаданные.СвойстваОбъектов.РежимЗаписиРегистра.Независимый);
	Результат.Вставить("ЭтоПодчиненныйРегистр", Результат.ЭтоРегистр И НЕ Результат.ЭтоНезависимыйРегистр);
	Результат.Вставить("ЭтоСсылочныйОбъект"	  , Не Результат.ЭтоКонстанта И НЕ Результат.ЭтоРегистр И НЕ Результат.ЭтоПоследовательность);
		
	КлючевыеПоля = Новый Массив;
	Если Результат.ЭтоКонстанта Тогда
	ИначеЕсли Результат.ЭтоСсылочныйОбъект Тогда
		КлючевыеПоля.Добавить("Ссылка");
	ИначеЕсли Результат.ЭтоПодчиненныйРегистр Или Результат.ЭтоПоследовательность Тогда
		КлючевыеПоля.Добавить("Регистратор");
	ИначеЕсли Результат.ЭтоРегистрСведений Тогда
		Если МетаданныеТаблицы.ПериодичностьРегистраСведений<>Метаданные.СвойстваОбъектов.ПериодичностьРегистраСведений.Непериодический И МетаданныеТаблицы.ОсновнойОтборПоПериоду Тогда
			КлючевыеПоля.Добавить("Период");
		КонецЕсли;
		Для Каждого Измерение Из МетаданныеТаблицы.Измерения Цикл
			Если Измерение.ОсновнойОтбор Тогда
				КлючевыеПоля.Добавить(Измерение.Имя);
			КонецЕсли;
		КонецЦикла;	
	Иначе
		ВызватьИсключение "Неожиданный вид таблицы.";	
	КонецЕсли;
	Результат.Вставить("КлючевыеПоля", Новый ФиксированныйМассив(КлючевыеПоля));
	
	//м = Новый Массив;
	//м.Добавить(МетаданныеТаблицы);
	//СтруктураХранения = ПолучитьСтруктуруХраненияБазыДанных(м, Истина);
	//Для Каждого с Из СтруктураХранения Цикл
	//	Если с.Назначение = "Основная" Тогда
	//		Результат.Вставить("ИмяТаблицыХранения", с.ИмяТаблицыХранения);
	//		Прервать;
	//	КонецЕсли;
	//КонецЦикла;
	
	ИмяТипа = Результат.Вид;
	Если Результат.ЭтоКонстанта Тогда
		ИмяТипа = ИмяТипа + "МенеджерЗначения";
	ИначеЕсли Результат.ЭтоСсылочныйОбъект Тогда
		ИмяТипа = ИмяТипа + "Объект";
	Иначе //регистр
		ИмяТипа = ИмяТипа + "НаборЗаписей";
	КонецЕсли;
	ИмяТипа = ИмяТипа + "." + Результат.Имя;
	Результат.Вставить("Тип", Тип(ИмяТипа));
	
	Возврат Новый ФиксированнаяСтруктура(Результат);
	
КонецФункции

Функция МенеджерТаблицы(Таблица) Экспорт

	Попытка
		Поз = Найти(Таблица, ".");
		ТаблицаВид = Лев(Таблица, Поз-1);
		ТаблицаИмя = Сред(Таблица, Поз+1);
		
		Если ТаблицаВид = "Справочник" Тогда
			Возврат Справочники[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "Документ" Тогда
			Возврат Документы[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "Последовательность" Тогда
			Возврат Последовательности[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "РегистрСведений" Тогда
			Возврат РегистрыСведений[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "РегистрНакопления" Тогда
			Возврат РегистрыНакопления[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "РегистрБухгалтерии" Тогда
			Возврат РегистрыБухгалтерии[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "Константа" Тогда
			Возврат Константы[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "БизнесПроцесс" Тогда
			Возврат БизнесПроцессы[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "Задача" Тогда
			Возврат Задачи[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "ПланВидовХарактеристик" Тогда
			Возврат ПланыВидовХарактеристик[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "ПланСчетов" Тогда
			Возврат ПланыСчетов[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "ПланВидовРасчета" Тогда
			Возврат ПланыВидовРасчета[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "РегистрРасчета" Тогда
			Возврат РегистрыРасчета[ТаблицаИмя];
		ИначеЕсли ТаблицаВид = "ПланОбмена" Тогда
			Возврат ПланыОбмена[ТаблицаИмя];
		Иначе
			//ВызватьИсключение "Неожиданная таблица: "+ТаблицаПолноеИмя;
			Возврат Неопределено;
		КонецЕсли;
	Исключение
		Возврат Неопределено;
	КонецПопытки;
		
КонецФункции

Функция ГруппаПолучателей() Экспорт
	
	Возврат КафкаПереопределяемый.ГруппаПолучателей();
	
КонецФункции

Функция ЭтоБэкап() Экспорт
	
	Возврат КафкаПереопределяемый.ЭтоБэкап();
	
КонецФункции

Функция УровеньЖурналаРегистрации() Экспорт
	
	УстановитьПривилегированныйРежим(Истина);
	
	МассивУровней = ПолучитьИспользованиеЖурналаРегистрации();
	
	Если МассивУровней.Найти(УровеньЖурналаРегистрации.Примечание) <> Неопределено Тогда
		Возврат УровеньЖурналаРегистрации.Примечание;
	ИначеЕсли МассивУровней.Найти(УровеньЖурналаРегистрации.Информация) <> Неопределено Тогда
		Возврат УровеньЖурналаРегистрации.Информация;
	ИначеЕсли МассивУровней.Найти(УровеньЖурналаРегистрации.Предупреждение) <> Неопределено Тогда
		Возврат УровеньЖурналаРегистрации.Предупреждение;
	ИначеЕсли МассивУровней.Найти(УровеньЖурналаРегистрации.Ошибка) <> Неопределено Тогда
		Возврат УровеньЖурналаРегистрации.Ошибка;
	КонецЕсли;
	
	Возврат Неопределено;
	
КонецФункции

Функция ИмяЗначенияПеречисления(Значение) Экспорт
	
	ОбъектМетаданных = Значение.Метаданные();
	
	ИндексЗначения = Перечисления[ОбъектМетаданных.Имя].Индекс(Значение);
	
	Возврат ОбъектМетаданных.ЗначенияПеречисления[ИндексЗначения].Имя;
	
КонецФункции

Функция ТипыПеречисления() Экспорт
	
	Результат = Новый Соответствие;
	
	Для Каждого МетаданныеПеречисления Из Метаданные.Перечисления Цикл
		Результат.Вставить(Тип("ПеречислениеСсылка."+МетаданныеПеречисления.Имя), Истина);
	КонецЦикла;
	
	Возврат Новый ФиксированноеСоответствие(Результат);
	
КонецФункции

Функция ТипыДляСериализацииXML() Экспорт
	
	Результат = КафкаКлиентСервер.ТипыДляСериализацииXML();
		
	Возврат Новый ФиксированнаяСтруктура("Соответствие, Массив",
					Новый ФиксированноеСоответствие(Результат.Соответствие),
					Новый ФиксированноеСоответствие(Результат.Массив));
	
КонецФункции