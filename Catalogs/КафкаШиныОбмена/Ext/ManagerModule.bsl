﻿
Функция ОбменРасписаниеПоУмолчанию() Экспорт
	
	ОбменРасписание = Новый РасписаниеРегламентногоЗадания();
	ОбменРасписание.ПериодПовтораДней = 1;
	ОбменРасписание.ПериодПовтораВТечениеДня = 300;
	
	Возврат ОбменРасписание;
	
КонецФункции