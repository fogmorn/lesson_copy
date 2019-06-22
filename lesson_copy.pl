#!perl
use strict;
use warnings;
use v5.16;
use Tk;
use Tk::DateEntry;
use Tk::Tree; 
use Tk::Image;
use Date::Calc qw/Add_Delta_Days
                  Delta_Days
                  Day_of_Week/;  
use Time::Piece; 
use DBI; 
use Switch 'Perl6';
use Encode;
# use Devel::Peek;
# use IsUTF8 qw(isUTF8); # проверка строки на соответствие кодировке utf-8

my $path = '';

# при вызове из Delphi указываем скрипту, что его корневой каталог
# расположен в "modules" (передаётся вторым аргументом из Delphi)
if (@ARGV) {
	$path = "$ARGV[1]" . "/";
}

# кол-во недель исходного диапазона (1 или 2) для копирования
my $num_src_weeks; 

# кол-во недель целевого диапазона для копирования
my $weeks_for_copy; 

# кол-во копий, которое будет произведено
my $num_copies;

# начальная дата исходного диапазона
my $begdate; 

# конечная дата исходного диапазона
my $enddate;

# начальная дата целевого диапазона
my $tgt_enddate; 

# конечная дата целевого диапазона
my $tgt_begdate; 


# список всех потоков
my @stream_list; 

# список `id' потоков, при копировании которых произошла ошибка
my @err_stream_id; 

# список `id' потоков, у которых нет занятий, но они были выбраны для копирования
my @no_lsn_stream_id; 

# признак занесения текущего потока в массив err_stream_id
my $err_flag = 0; 

#
# В случае, если для какого-либо из потоков целевой диапазон содержит данные,
# копирование остальных потоков не выполняется, 
# сообщения об ошибке/успехе не выводятся.
#
my $deny_copying = 0; 

my $host;
my $user;


open FILE, "<", "$path" . "dbconnect.conf" or die $!;


while (my $line = <FILE>) {
	given ( $line ) {
		when m/host/ {
			(my $val, $host) = split('=', $line);
		}
		when m/user/ {
			(my $val, $user) = split('=', $line);
		}
	}
}

close FILE;

chomp($host);
chomp($user);

# Подключение к БД: $data_src, $username, $password
# $dbh - дескриптов БД
my $dbh = DBI->connect
	( "DBI:mysql:lesson:$host", "$user", "lesson" )
	or die( "Error connecting to database: $DBI::errstr\n" );


# Устанавливаем кодировку, в которой будем посылать запросы
$dbh->do("set names 'cp1251'");


# Получаем код активного периода для последующего получения списка потоков
my $sql = "SELECT param 
             FROM settings
               WHERE description
                 LIKE '%Код активного периода%'";


my $sth = $dbh->prepare($sql);
$sth->execute();

my $periodid = $sth->fetchrow();

$sth->finish();


# Получаем `id' потоков
$sql = "SELECT s.id AS id, 
          GROUP_CONCAT(
          CONCAT(groupname.name, 
            IF(subgroup='','',CONCAT('.',subgroup)) 
            )
          SEPARATOR ', ') AS groups, 
          s.name AS name
        FROM 
        (SELECT id,name FROM stream 
	     WHERE periodid='$periodid') AS s 
        LEFT JOIN streamgroup ON s.id=streamgroup.streamid 
        LEFT JOIN groups ON streamgroup.groupid=groups.id 
        LEFT JOIN groupname ON groups.groupnameid=groupname.id 
        GROUP BY s.id 
	    ORDER BY GROUP_CONCAT(
          CONCAT(groupname.name, 
            IF(subgroup='','',CONCAT('.',subgroup)) 
            )
          SEPARATOR ', ')
";

$sth = $dbh->prepare($sql);
$sth->execute();

my $group_rows = $sth->fetchall_arrayref({});

$sth->finish();


#
# Используется для отображения кириллицы в GUI
# (перенесено сюда, т.к. не удавалось получить "код активного периода")
#
use encoding "cp1251";



##############################################
##      
##    GUI
##
##############################################


# Создаём главное окно программы
my $mw = MainWindow->new;

# Параметры главного окна
$mw->geometry('850x450');
$mw->title('Копирование расписания');

# Шрифт по умолчанию для всех виджетов
$mw->optionAdd('*font', 'Verdana 10'); 


$mw->Label(
	-text => 'Поток',
	-font => 'Verdana 12 bold')
->place(-x => 10, -y => 20);



##
## "Tree" для отображения списка потоков
##

my $tree = $mw->Scrolled(
	'Tree',
	-header             => 'true',
	-columns            => '2',
	-width              => '50',
	-height             => '16',
	-selectborderwidth  => '0',
	-highlightthickness => '0',
	-relief             => 'groove', # raised,sunken,flat,ridge,solid,groove
	-scrollbars         => 'se',
	-background         => '#ffffff',
	-selectforeground   => '#ffffff',
	-selectbackground   => '#0a246a',
	-selectmode         => 'extended',
	-font               => 'Verdana 10')
->place(-x => 10, -y => 50);

$tree->header('create',
			  '0',
			  -itemtype => 'text',
			  -text     => 'Группы');

$tree->header('create',
			  '1',
			  -itemtype => 'text',
			  -text     => 'Наименование');



# После отрисовки "Tree" задаём иконку окна
my $icon = $mw->Photo(-file => "$path" . "img/icon32.gif");
$mw->Icon(-image => $icon);



# Заполнение виджета "Tree" списком потоков
foreach my $row (@$group_rows) {

	my $groups = decode('cp1251', "$row->{groups}");
	my $name   = decode('cp1251', "$row->{name}");

	# корень
	$tree->add("$row->{id}");
	
	# колонка 1 - Группы
	$tree->itemCreate("$row->{id}",                   # path
					  '0',                            # col id
					  -itemtype => 'text',            # тип  - text/data...
					  -text     => "$groups");        # текст для отображения
	
	# колонка 2 - Наименование
	$tree->itemCreate("$row->{id}",
					  '1',
					  -itemtype => 'text',
					  -text     => "$name");
}



##
## Исходный диапазон дат (откуда копируются занятия)
##

# Фрейм для виджетов исходного диапазона
my $src_frame = $mw->Frame(-borderwidth => '1',
						   -height      => '95',
						   -width       => '370',
						   -relief      => 'solid')
->place(-x => 450, -y => 50);


$src_frame->Label(-text => 'Количество недель для копирования:',
				  -font => 'Verdana 10 bold')
->place(-x => 5, -y => 5);


$src_frame->Label(-text => "с")->place(-x => 5, -y => 45);


#
# Виджет "Календарь" для начальной даты исходного диапазона
# (выпадающий календарь с Entry для отображения выбранной даты)
#
my $begdate_Entry = $src_frame->DateEntry(
	-disabledbackground => '#ffffff',
	-disabledforeground => '#000000',
	-weekstart          => 1,
	-textvariable       => \$begdate,
	-state              => 'readonly',
	-daynames           => [qw/Вс Пн Вт Ср Чт Пт Сб/],
	-todaybackground    => '#bebebe',
	-buttonbackground   => '#fbf4eb',
	-boxbackground      => '#bebebe', #d4d0c8
	-execcmd            => sub { &calc_dates('begdate_Entry'); },
	-formatcmd          => sub { sprintf("%02d.%02d.%04d",$_[2],$_[1],$_[0]); }
)->place(-x => 25, -y => 47);


$src_frame->Label(-text => "по")->place(-x => 145, -y => 45);

#
# "Entry" для конечной даты исходного диапазона
#
my $enddate_Entry = $src_frame->Entry(
	-disabledbackground => '#ffffff',
	-disabledforeground => '#000000',
	-width              => 10,
	-state              => 'disabled',
	-textvariable       => \$enddate)
->place(-x => 175, -y => 47);




##
## Целевой диапазон дат (куда происходит вставка скопированных занятий)
##

# Фрейм для виджетов целевого  диапазона
my $tgt_frame = $mw->Frame(-borderwidth => '1',
						   -height      => '170',
						   -width       => '370',
						   -relief      => 'solid')
->place(-x => 450, -y => 160);


$tgt_frame->Label(-text => 'Копировать в:',
				  -font => 'Verdana 10 bold')
->place(-x => 5, -y => 5);


$tgt_frame->Label(-text => "с")->place(-x => 5, -y => 45);



#
# Виджет "Календарь" для начальной даты целевого диапазона
# (выпадающий календарь с Entry для отображения выбранной даты)
#
my $tgt_begdate_Entry = $tgt_frame->DateEntry(
	-disabledbackground => '#ffffff',
	-disabledforeground => '#000000',
	-weekstart          => 1,
	-textvariable       => \$tgt_begdate,
	-state              => "readonly",
	-daynames           => [qw/Вс Пн Вт Ср Чт Пт Сб/],
	-todaybackground    => '#bebebe',
	-buttonbackground   => '#fbf4eb', 
	-boxbackground      => '#bebebe', #d4d0c8
	-execcmd            => sub { &calc_dates('tgt_begdate_Entry'); },
	-formatcmd          => sub { sprintf("%02d.%02d.%04d",$_[2],$_[1],$_[0]); }
)->place(-x => 25, -y => 47);


$tgt_frame->Label(-text => 'по')->place(-x => 145, -y => 45);


#
# Кнопка "<"
#
# В зависимости от кол-ва исходных недель, отнимаем/прибавляем 7 или 14 дней, 
# т.е. изменяем конечный диапазон на 1 или 2 недели
#
my $decr_btn = $tgt_frame->Button(
	-text    => '<',
	-command => sub { &calc_dates('<'); } )
->place(-x => 175, -y => 43);



#
# Entry для конечной даты целевого диапазона 
#
my $tgt_enddate_Entry = $tgt_frame->Entry(
	-disabledbackground => '#ffffff',
	-disabledforeground => '#000000',
	-width              => 10,
	-state              => 'disabled',
	-textvariable       => \$tgt_enddate )
->place(-x => 198, -y => 45);



# Кнопка ">"
my $incr_btn = $tgt_frame->Button(
	-text    => '>',
	-command => sub { &calc_dates('>'); } )
->place(-x => 286, -y => 43);



#
# Виджет для выбора кол-ва недель исходного диапазона
#
$src_frame->Optionmenu(
	-options  => [qw/2 1/],
	-variable => \$num_src_weeks,
	-command  => \&select_num_src_weeks)
->place(-x => 300, -y => 3);



# Кнопки
my $closeImage = $mw->Photo(-file => "$path" . "img/close.gif");
my $applyImage = $mw->Photo(-file => "$path" . "img/apply.gif");

$mw->Button(
	-width    => '120',
	-height   => '25',
	-compound => 'left',
	-font     => 'Verdana 10 bold',
	-text     => '  Закрыть',
	-image    => $closeImage,
	-command  => sub { $mw->destroy; } )
->place(-x => 470, -y => 370); # x => 280

$mw->Button(
	-width      => '140',
	-height     => '25',
	-image      => $applyImage,
	-compound   => 'left',
	-font       => 'Verdana 10 bold',
	-text       => '  Скопировать',
	-foreground => 'black',
	-command    => \&copy_lessons,)
	#-state      => 'disabled')
->place(-x => 280, -y => 370); # x => 470


#
# Информация о копировании (кол-во недель и кол-во копий)
#
$tgt_frame->Label(-text => 'Количество недель:')
->place(-x => 10, -y => 100);


$tgt_frame->Label(
	-text => 'Количество копий:')
->place(-x => 10, -y => 125);

my $weeks_for_copy_Enrty = $tgt_frame->Entry(
	-disabledbackground => '#ffffff',
	-disabledforeground => '#000000',
	-width              => 3,
	-state              => 'disabled',
	-textvariable       => \$weeks_for_copy)
->place(-x => 155, -y => 100);

my $num_copies_Entry = $tgt_frame->Entry(
	-disabledbackground => '#ffffff',
	-disabledforeground => '#000000',
	-width              => 3,
	-state              => 'disabled',
	-textvariable       => \$num_copies)
->place(-x => 155, -y => 125);


MainLoop;


##############################################
##
##    Подпрограммы
##
##############################################


##
## Проверяет, является ли день недели заданной даты понедельником
##
sub check_weekday {
    #
    # Возвращает строку даты и её разбор (год/месяц/день).
    #
	
	my $sub_begdate = $_[0];
	my ($y,$m,$d, $offset);


	($sub_begdate, $y,$m,$d) = &parse_date($sub_begdate,
										   '%d.%m.%Y',
										   '.');
	
	# определяем день недели выбранной даты
	my $sub_weekday = Day_of_Week($y,$m,$d);
	
	
	# если выбран не понедельник - исправляем на понедельник той же недели,
	# которой принадлежит выбранная дата
	if ($sub_weekday != 1) {
		
		$offset = ($sub_weekday-1)*(-1);
		
		($sub_begdate,$y,$m,$d) = &add_days($sub_begdate,
											$y,$m,$d,
											$offset);		
		
		$mw->messageBox(
			-icon    => 'info',
			-message => "Период для копирования должен начинаться" .
			" c понедельника!\n\n" . 
			"Исправлено на: " . 
			sprintf("%02d.%02d.%04d",$d,$m,$y),
			-type    => 'Ok',
			-default => 'Ok');
	}
	return $sub_begdate,$y,$m,$d;
}



## 
## Процедура "Вычисление конечной даты".
## 
sub calc_dates {
	#
    # Принимает параметр "поле" (виджет, из которого была вызвана процедура).
    # 
    # На основе параметра происходит определение того, какую операцию 
    # следует выполнить.
    # 
    # Возвращает различные значения, в зависимости от операции.
    # 
	
	my $field = $_[0];
	my $sub_begdate;
	my $sub_enddate;
	
    # ссылки на переменные для изменения значений полей дат
	my $link_begdate;
	my $link_enddate;
	
	my ($y,$m,$d,$offset);
	
	given ($field) {
		
		# подготовительные действия для вычисления конечной даты 
		# исходного диапазона
		when 'begdate_Entry' { 
			
			$sub_begdate = $begdate;
			$sub_enddate = $enddate;

			$link_begdate = \$begdate;
			$link_enddate = \$enddate;
			
			# очищаем поля дат целевого диапазона
			$tgt_begdate = '';
			$tgt_enddate = '';
			
			next;
		}
		
		# работа с целевым диапазоном отличается тем, что происходит проверка
		# на отсутствие пересечения с исходным
		when 'tgt_begdate_Entry' {

			$sub_begdate = $tgt_begdate;
			$sub_enddate = $tgt_enddate;
			
			$link_begdate = \$tgt_begdate;
			$link_enddate = \$tgt_enddate;
			
			my ($se_y,$se_m,$se_d) = &parse_date($enddate,
												 '%d.%m.%Y',
												 '.',
												 '-nodate');
			my ($tb_y,$tb_m,$tb_d) = &parse_date($sub_begdate,
												 '%d.%m.%Y',
												 '.',
												 '-nodate');
			
			# Если начальная целевого меньше конечной исходного,
			# то диапазоны пересекаются, а такого быть не должно
			if ( (Delta_Days($se_y,$se_m,$se_d,
							 $tb_y,$tb_m,$tb_d))  < 0 ) { 

				$mw->messageBox(
					-icon    => 'error',
					-message => "Начальная дата должна быть больше $enddate!",
					-type    => 'Ok',
					-default => 'Ok');
				
				# очищаем начальную дату целевого диапазона 
				$$link_begdate = '';
				
				return;
			}
			
			# если всё в порядке, идём дальше
			next;			
		}
		
        # вычисление конечной даты (для исходного/целевого диапазонов)
		when m/tgt_begdate_Entry|begdate_Entry/ {
			
			# если в качестве начальной даты выбран не понедельник -
			# исправляем нечальную дату на понедельник
			($sub_begdate,$y,$m,$d) = &check_weekday($sub_begdate);
			
			# для вычисления конечной даты прибавляем к начальной дате
			# количество недель исходного диапазона
			$offset = ($num_src_weeks * 7)-1;
			
			($sub_enddate,$y,$m,$d) = &add_days($sub_enddate,$y,$m,$d,$offset);
			
			# изменяем значения в полях дат по ссылке
			$$link_begdate = $sub_begdate;
			$$link_enddate = $sub_enddate;
			
			next;
		}
		
		# если вызывается нажатием одной из кнопок - "<" / ">",
		# вычисляем конечную дату 
		when m/<|>/ {
			
			$sub_enddate = $tgt_enddate;

			# переменная, в зависимости от которой прибавляем или отнимаем дни
			my $move;

			if ($field eq '<') { $move = -1 }
			else { $move = 1 }

			($sub_enddate,$y,$m,$d) = &parse_date($sub_enddate,'%d.%m.%Y','.');

			$offset = ($move * ($num_src_weeks * 7));
			
			($sub_enddate,$y,$m,$d) = &add_days($sub_enddate,$y,$m,$d,$offset);
			
			$tgt_enddate = $sub_enddate;
			
			next;			
		}

		# заполняем информационные поля
		when m/tgt_begdate_Entry|<|>/ {
			
			my $link_weeks_for_copy = \$weeks_for_copy;
			my $link_num_copies = \$num_copies;

			my ($tb_y,$tb_m,$tb_d) = &parse_date($tgt_begdate,
												 '%d.%m.%Y',
												 '.',
												 '-nodate');
			
			my ($te_y,$te_m,$te_d) = &parse_date($tgt_enddate,
												 '%d.%m.%Y',
												 '.',
												 '-nodate');
			$weeks_for_copy = 
				( 1 + (Delta_Days($tb_y,$tb_m,$tb_d,$te_y,$te_m,$te_d)) ) / 7;
			
			$num_copies = ($weeks_for_copy / $num_src_weeks);
			
			return $$link_weeks_for_copy = $weeks_for_copy, 
			$$link_num_copies = $num_copies;

		}		
	}

	return;

}



## 
## Процедура разбора (парсинга) строки с датой
## 
sub parse_date {
    # Принимаемые параметры: 
    #  - строка даты,
    #  - формат, который следует применить к дате: "%d.%m.%Y" или "%d-%m-%Y"
    #  - разделитель: "-" или "."
    # 
    # Разбирает дату и форматирует её в зависимости от формата и разделителя.
    # 
    # Возвращает преобразованную строку с датой (по умолч.)
    # и её разбор - год/месяц/день.
    # 

	my $date = $_[0];
	my $format = $_[1];
	my $split_char = $_[2];
	my ($y,$m,$d);

	# разбираем
	$date = Time::Piece->strptime($date,"%d.%m.%Y");

	# форматируем
	$date = $date->strftime($format);

	if ($split_char eq '-') { 
		($y,$m,$d) = split('\\' . $split_char, $date); 
	}
	else { 
		($d,$m,$y) = split('\\' . $split_char, $date); 
	}

    # аргумент "-nodate" (если дата нам не нужна)
	if ($_[3]) {  
		return $y,$m,$d;
	}


	return $date,$y,$m,$d;

}



## 
## Прибавляет к дате заданное количество дней
## 
sub add_days {
	#
	# Процедура работает в связке с `calc_dates'.
	#
    # Принимает дату, год, мессяц, день, сдвииг($offset).
    # В зависимости от сдвига (+/-) прибавляет или отнимает дни от даты.
    # Возвращает дату, год, месяц, день.
    # 

	my ($date,$y,$m,$d,$offset) = @_;

	($y,$m,$d) = Add_Delta_Days($y,$m,$d, $offset);

	# sprintf - используется для форматированного вывода в переменную
	$date = sprintf("%02d.%02d.%04d",$d,$m,$y);
	
	return $date,$y,$m,$d;

}



## 
## Процедура, отрабатывающая при выборе кол-ва недель исходного диапазона (1/2)
## в виджете "Option menu".
## 
sub select_num_src_weeks {

	# запрещаем изменять конечную дату целевого диапазона, если неделя одна
	if ( $num_src_weeks == 1) {
		$incr_btn->configure(-state => 'disabled');
		$decr_btn->configure(-state => 'disabled');
	}
	else {
		$incr_btn->configure(-state => 'normal');
		$decr_btn->configure(-state => 'normal');
	}

	# если начальная дата исходного диапазона существует (т.е. выбрана)
	# проиводим вычисление конечной в соответствии с изменившимся кол-вом недель
	if ($begdate) { &calc_dates('begdate_Entry'); }

	$tgt_begdate = '';
	$tgt_enddate = '';
	$weeks_for_copy = '';
	$num_copies = '';
}



## 
## Основная процедура "Копирование"
## 
## Получает данные заполненных полей дат методом get() и список выбранных
## потоков; для каждого потока запускает процедуру копирования.
## 
## Возвращает сообщение об успехе/ошибке. Если целевой диапазон не пуст, 
## не возвращает ничего.
## 
sub copy_lessons {
	
	my $begdate = $begdate_Entry->get();
	my $enddate = $enddate_Entry->get();
	my $tgt_begdate = $tgt_begdate_Entry->get();
	my $tgt_enddate = $tgt_enddate_Entry->get();

	# `id' выбранных потоков, имена выбранных потоков, 
    # выбранные элементы (виджета tree)
	my (@sel_stream_id, @sel_stream_name, @selitems);

    # имена потоков, при копировании которых произошла ошибка
	my @err_stream_name;

	# имена потоков, у которых нет занятий в данный период, но они были выбраны
	my @no_lsn_stream_name;


    # получаем список выбранных элементов (потоков) виджета Tree
	@selitems = $tree->infoSelection;


	# проверка существования данных, неоходимых для копирования
	if ( $begdate eq '' || $tgt_begdate eq '' || $#selitems+1 < 0 ) {
		$mw->messageBox(
			-icon    => 'warning',
			-message => "Недостаточно данных для копирования.",
			-type    => 'Ok');
		return;
	}


    # очищаем массив путём присваивания значения "-1" 
    # последнему элементу массива ($#)
	$#err_stream_id = -1;
	$#no_lsn_stream_id = -1;
	
	# получаем `id' и `имя' для каждого элемента
	foreach my $path (@selitems) {

		# в $path хранится `id' потока
		push @sel_stream_id, $path;
		
		push @sel_stream_name, $tree->itemCget($path, 0, -text);
	
	}
	
	my $msg_result = $mw->messageBox(
		-icon    => 'question',
		-message => "Будет выполнено копирование расписания для потоков:\n\n" . 
	            	join("\n", @sel_stream_name) . 
		            "\n\nПосле выполнения операции отмена изменений " .
		            "будет невозможна.\n\nПродолжить?",
		-type    => 'YesNo',
		-default => 'No');


	if ($msg_result eq 'No') { return; }

    # проверяем на наличие занятий у каждого потока в целевом диапазоне
	foreach my $id (@sel_stream_id) {
		my $sql = "SELECT COUNT(*)
                 FROM lessons 
                   WHERE streamid='$id' 
                     AND lessdate>=STR_TO_DATE('$tgt_begdate','%d.%m.%Y')
                     AND lessdate<=STR_TO_DATE('$tgt_enddate','%d.%m.%Y')";

		my $sth = $dbh->prepare($sql);
		$sth->execute();

		my $count;

		while(my @row = $sth->fetchrow_array()){
			$count = $row[0];
		}

		if ($count != 0) { 
			
			# в случае, если для других потоков целевой диапазон пуст
			# и можно копировать, всё равно запрещаем их копирование
			$deny_copying = 1;
			
			$mw->messageBox(
				-icon    => 'warning',
				-message => "У потока " . $tree->itemCget($id, 0, -text). 
				" в период с $tgt_begdate по $tgt_enddate\n" . 
				"уже есть занятия (кол-во занятий: $count).\n\n" . 
				"Копирование не может быть выполнено.",
				-title   => 'Внимание!',
				-type    => 'Ok',
				-default => 'Ok');
		}
	}
	
    # если для какого-либо из потоков диапазон не пуст, выходим
	if ( $deny_copying != 0 ) { return $deny_copying = 0; }
	
	# для каждого `id' потока вызываем подпрограмму копирования
	foreach my $id (@sel_stream_id) {
		
		my $stream_id = $id;

		my $stream_name = $tree->itemCget($id, 0, -text);

		&exec_copying($stream_id, $begdate, 
					  $enddate, $tgt_begdate, 
					  $tgt_enddate,$stream_name);
	}
	

	#
	# В зависимости от результата выполнения процедуры копирования
	# выполняем действия:
	#
	if ( $#no_lsn_stream_id+1 != 0 ) {
			foreach my $stream_id (@no_lsn_stream_id) {
				push @no_lsn_stream_name, $tree->itemCget($stream_id, 0, -text);
			}

			$mw->messageBox(-icon    => 'warning',
							-message => 
							"У следующих потоков в выбранный период " .
							"($begdate - $enddate) нет занятий:\n" .
							"\"" . join("\n", @no_lsn_stream_name) . "\"",
							-type    => 'Ok',
							-default => 'Ok');
	}

	# при копировании произошли ошибки - выводим сообщение
	if ( $#err_stream_id+1 != 0 ) { # индекс последнего элемента массива 
   	                                # (размер массива - 1)
			foreach my $stream_id (@err_stream_id) {
				push @err_stream_name, $tree->itemCget($stream_id, 0, -text);
			}

			$mw->messageBox(-icon    => 'warning',
							-message => 
							"Копирование завершено с ошибками для потоков:\n" .
							"\"" . join("\n", @err_stream_name) . "\"" . 
							"\n\nСмотрите отчёт в каталоге \"/log\".",
							-type    => 'Ok',
							-default => 'Ok');
	}
	# если копирование упешно - сообщение об успехе
	else {
		$mw->messageBox(-icon    => 'info',
						-message => "Копирование завершено успешно.",
						-type    => 'Ok',
						-default => 'Ok');
	}

	return;

}



## 
## Копирование расписания для заданного потока 
## 
sub exec_copying {
    
    # Принимает:
    #  - `id' потока
    #  - начальную дату исходного диапазона
    #  - конечную дату исходного диапазона
    #  - начальную дату целевого диапазона
    #  - конечную дату целевого диапазона
    #  - имя потока (для формирования имени файла)

	my ($stream_id, $begdate, $enddate, $tgt_begdate, 
		$tgt_enddate,$stream_name) = @_;

	#my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);

	my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);

	# имя файла для записи 
	my $filename = sprintf("%04d-%02d-%02d_%02d-%02d-%02d_",
						   $year+1900,$mon+1,$mday,$hour,$min,$sec);


	$filename .= $stream_name; # "приклеиваем" имя потока

    # переменные год/месяц/день для дат исходного диапазона
	my $src_begdate; 
	my ($b_y, $b_m, $b_d);
	my ($e_y, $e_m, $e_d);

    # переменные год/месяц/день для дат целевого диапазона
	my ($tb_y, $tb_m, $tb_d);
	my ($te_y, $te_m, $te_d);

	my %hash = ();

	my $date_index = 1;
	my $err_flag = 0;

	# информация об очередном скопированном занятии
	my $periodid;
	my $pairid;
	my $roomid;
	my $tutorid;
	my $lesstypeid;
	my $subjid;

	# очередная дата вставки очередного скопированного занятия (вычисляется)
	my $lessdate;
	
	# путь к файлу лога
	my $filepath = "$path" . "log/$filename.txt";

	# переводим из внутреннего формата (Perl Internal Format) в ср1251 
    # для корректного отображения имени потока в имени файла
	my $filepath_enc = encode('cp1251', $filepath);
	
	open DATA, ">>", $filepath_enc 
		or die ($mw->messageBox(-icon    => 'error',
								-message => "Не могу открыть файл для записи." .
								"\nОшибка: Нет такого файла или каталога.",
								-type    => 'Ok',
								-default => 'Ok')
		);

	binmode(DATA, ":utf8"); # Указание кодировки для файлового дескриптора 
                            # (иначе возникает Warning: 
                            # "Wide character in print" at print DATA)
	
	
	($begdate, $b_y, $b_m, $b_d) = &parse_date($begdate,
											   '%Y-%m-%d',
											   '-');
	
	($enddate, $e_y, $e_m, $e_d) = &parse_date($enddate,
											   '%Y-%m-%d',
											   '-');

	($tgt_begdate, $tb_y, $tb_m, $tb_d) = &parse_date($tgt_begdate,
													  '%Y-%m-%d',
													  '-');

	($tgt_enddate, $te_y, $te_m, $te_d) = &parse_date($tgt_enddate,
													  '%Y-%m-%d',
													  '-');

	# кол-во дней исходного диапазона
	my $num_src_date = $num_src_weeks * 7;
	my $key;

	
	#
	# Для каждой даты исходного диапазона определяем все соответствующие ей 
    # даты целевого диапазона.
	#
	# Заполняем hash: $hash{ ($src_begdate,$date_index) } -> $tgt_begdate
	#
    # Например:
    #    2015-01-01,1 -> 2015-01-15
	#    2015-01-01,2 -> 2015-01-27 (+14)
	#    2015-01-02,1 -> 2015-01-16
	#    2015-01-02,2 -> 2015-01-28 ...
	#
	for ( my $i=1; $i<=$num_src_date; $i++ ) {
		
		#
		# находим первое соответствие
		#		
		$date_index = 1;

		$key = sprintf("%04d-%02d-%02d,%d",
					   $b_y,$b_m,$b_d,$date_index);
		
		$tgt_begdate = sprintf("%04d-%02d-%02d",
							   $tb_y,$tb_m,$tb_d);

		$hash{$key} = $tgt_begdate;

		
		my ($loop_y,$loop_m,$loop_d) = ($tb_y,$tb_m,$tb_d);


		# находим все остальные соответствия для данной даты 
		# (в случае, если кол-во копий больше одной)
		for ( my $date_index=2; $date_index<=$num_copies; $date_index++ ) {
			
			$key = sprintf("%04d-%02d-%02d,%d",
						   $b_y,$b_m,$b_d,$date_index);
	
			# прибавляем к дате целевого диапазона кол-во дней исходного (7/14)
			($loop_y, $loop_m, $loop_d) = Add_Delta_Days(
	                       $loop_y, $loop_m, $loop_d, $num_src_date);

			$tgt_begdate = sprintf("%04d-%02d-%02d",
								   $loop_y,$loop_m,$loop_d);

			$hash{$key} = $tgt_begdate;
		
		}

		# приступаем к следующей дате
		($b_y, $b_m, $b_d) = Add_Delta_Days($b_y,$b_m,$b_d, 1);

		($tb_y,$tb_m,$tb_d) = Add_Delta_Days($tb_y,$tb_m,$tb_d, 1);

	}

	
	#
	# Запрашиваем данные о расписании для текущего потока (копируем)
	#
	$sql = "SELECT * 
              FROM lessons 
              LEFT JOIN pair ON lessons.pairid=pair.id
                WHERE streamid='$stream_id'
                  AND lessdate>='$begdate' 
                  AND lessdate<='$enddate'            
            ORDER BY lessdate,numpair";
	
	$sth = $dbh->prepare($sql);
	$sth->execute();

	my $rows = $sth->fetchall_arrayref({});

	# если у потока нет занятий
	if ( @$rows == 0 ) { 
		print DATA "\nУ потока $stream_id в выбранный период " .
		"($begdate - $enddate) занятий нет.\n";
		close DATA;
		$sth->finish();
		push @no_lsn_stream_id, $stream_id;
		return;
	}
	
	#
	# Подготовка запроса для вставки занятий
	#
	$sql = "INSERT INTO 
              lessons(periodid,
                      pairid,
                      roomid,
                      tutorid,
                      lesstypeid,
                      subjid,
                      streamid,
                      lessdate)
            VALUES (?,?,?,?,?,?,?,?)";


	$sth = $dbh->prepare($sql)
		or die($mw->messageBox(-icon    => 'error',
							   -message => "Не могу подготовить запрос" . 
							   " $DBI::errstr",
							   -type    => 'Ok',
							   -default => 'Ok')
		);
	

	# hash{$src_begdate,$date_index}
	#        2015-10-27,1
	#        2015-10-27,2
	$src_begdate = sprintf("%04d-%02d-%02d,%d", 
						   $b_y,$b_m,$b_d,$date_index);


	#
	# Вставка скопированных занятий в БД
	#
	for my $row (@$rows) {
		for (my $date_index=1; $date_index<=$num_copies; $date_index++) {
			
			$lessdate = $hash{"$row->{lessdate},$date_index"};

			# выводим текст запроса в лог
			print DATA "\nINSERT INTO lessons(periodid,pairid,roomid,tutorid,lesstypeid,subjid,streamid,lessdate)\n VALUES ($row->{periodid},$row->{pairid},$row->{roomid},$row->{tutorid},$row->{lesstypeid},$row->{subjid},$stream_id,$lessdate)\n";
			# eval die - try catch
			eval {

				$sth->execute($row->{periodid},$row->{pairid},
									 $row->{roomid},$row->{tutorid},
									 $row->{lesstypeid},$row->{subjid},
									 $stream_id,$lessdate) 
					or die;
			};

			if ($@) {
				print DATA "Ошибка при копировании: $DBI::errstr\n";
				$err_flag = 1;
			}
		}
	}

	close DATA;

	$sth->finish();
	
	if ($err_flag == 1) { push @err_stream_id, $stream_id; }
	
	return;
}

$dbh->disconnect();
