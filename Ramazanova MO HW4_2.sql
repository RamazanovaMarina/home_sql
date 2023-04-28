--=============== МОДУЛЬ 6. POSTGRESQL =======================================
--= ПОМНИТЕ, ЧТО НЕОБХОДИМО УСТАНОВИТЬ ВЕРНОЕ СОЕДИНЕНИЕ И ВЫБРАТЬ СХЕМУ PUBLIC===========
SET search_path TO public;

--======== ОСНОВНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Напишите SQL-запрос, который выводит всю информацию о фильмах 
--со специальным атрибутом "Behind the Scenes".
explain analyze
select film_id,
special_features
from film f
where'Behind the Scenes' = any (special_features)


--ЗАДАНИЕ №2
--Напишите еще 2 варианта поиска фильмов с атрибутом "Behind the Scenes",
--используя другие функции или операторы языка SQL для поиска значения в массиве.
explain analyze
select film_id,
special_features
from film f
where special_features && array ['Behind the Scenes']

explain analyze
select film_id,
special_features
from film f
where special_features @> array ['Behind the Scenes']






--ЗАДАНИЕ №3
--Для каждого покупателя посчитайте сколько он брал в аренду фильмов 
--со специальным атрибутом "Behind the Scenes.

--Обязательное условие для выполнения задания: используйте запрос из задания 1, 
--помещенный в CTE. CTE необходимо использовать для решения задания.
explain analyze 
with cte as 
(
select film_id,
special_features
from film f
where'Behind the Scenes' = any (special_features))
select r.customer_id, 
count (rental_id)
from rental r
join inventory i on i.inventory_id =r.inventory_id 
join cte on cte.film_id= i.film_id 
group by r.customer_id 
order by r.customer_id 

--ЗАДАНИЕ №4
--Для каждого покупателя посчитайте сколько он брал в аренду фильмов
-- со специальным атрибутом "Behind the Scenes".

--Обязательное условие для выполнения задания: используйте запрос из задания 1,
--помещенный в подзапрос, который необходимо использовать для решения задания.


 
explain analyze
select r.customer_id,  
 count(r.rental_id)
from rental r
join inventory i on i.inventory_id =r.inventory_id 
join (
select film_id,
special_features
from film f
where'Behind the Scenes' = any (special_features) 
 ) m  on m.film_id= i.film_id 
group by r.customer_id 
order by r.customer_id




--ЗАДАНИЕ №5
--Создайте материализованное представление с запросом из предыдущего задания
--и напишите запрос для обновления материализованного представления
create materialized view MV as 
(select film_id,
special_features,
'Behind the Scenes' = any (special_features)
from film f
)
with no data 
refresh materialized view MV 

--ЗАДАНИЕ №6
--С помощью explain analyze проведите анализ скорости выполнения запросов
-- из предыдущих заданий и ответьте на вопросы:

--1. Каким оператором или функцией языка SQL, используемых при выполнении домашнего задания, 
--   поиск значения в массиве происходит быстрее
-- при выполнении explain analyze в трех первых запросах получаем след. показатели    
--Execution time: 1= 0.344; 2= 0.427; 3=0.402
-- cost: 1=77.50; 2=67.50; 3=67.50
-- actial time 1=0.306; 2=0.393; 3=0.402
-- по данным показателям можно сделать вывод о том, что первый запрос работает быстрее, но более затратен последующие запросы 
-- работают дольше, но менее энерго затратны
-- Следовательно быстрее работает запрос из задания 1
 
--2. какой вариант вычислений работает быстрее: 
--   с использованием CTE или с использованием подзапроса
-- при выполнении запросов с cte мы получаем более быструю работу запроса, которая более энергозатратна
-- при выполнении запроса с подзапросом время выполнения увеличивается в два раза, но уменьшается энергозатратнась 
-- вариант запроса м Cte работает в два раза быстрее 


--======== ДОПОЛНИТЕЛЬНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Выполняйте это задание в форме ответа на сайте Нетологии
explain analyze
select distinct cu.first_name  || ' ' || cu.last_name as name, 
	count(ren.iid) over (partition by cu.customer_id)
from customer cu
full outer join 
	(select *, r.inventory_id as iid, inv.sf_string as sfs, r.customer_id as cid
	from rental r 
	full outer join 
		(select *, unnest(f.special_features) as sf_string
		from inventory i
		full outer join film f on f.film_id = i.film_id) as inv 
		on r.inventory_id = inv.inventory_id) as ren 
	on ren.cid = cu.customer_id 
where ren.sfs like '%Behind the Scenes%'
order by count desc
--При выполнении анализа данного запроса мы видим что он выполняется очень долго и очень затратеый, 
--особенно тяжелым в этом запросе является объединение которое дает огромное количество строк с которым сложно работать  
--  458100 строки при full join

--ЗАДАНИЕ №2
--Используя оконную функцию выведите для каждого сотрудника
--сведения о самой первой продаже этого сотрудника.
select t.staff_id,
       c.first_name,
       c.last_name,
       t.payment_date,
       t.amount
from ( 
select 
*, 
row_number () over (partition by staff_id order by payment_date)
from payment p) t 
join customer c on c.customer_id = t.customer_id
 where row_number =1 



--ЗАДАНИЕ №3
--Для каждого магазина определите и выведите одним SQL-запросом следующие аналитические показатели:
-- 1. день, в который арендовали больше всего фильмов (день в формате год-месяц-день)
-- 2. количество фильмов взятых в аренду в этот день
-- 3. день, в который продали фильмов на наименьшую сумму (день в формате год-месяц-день)
-- 4. сумму продажи в этот день
with x as (
select p.customer_id, 
       count(payment_id),
       sum(amount),
       max(payment_date)
from payment p
group by p.customer_id
), y as (
select c.country_id ,
       c.country,
       concat_ws(' ',c3.first_name  , c3.last_name) as name,
       c3.email,
       x.*
from country c
join city c2 on c2.country_id=c.country_id
join address a on a.city_id=c2.city_id
left join customer c3 on c3.address_id=a.address_id
left join x on x.customer_id = c3.customer_id
), zl as (
select 
     distinct on(country_id)
     country_id,
     country,
     name 
from y 
order by country_id, count desc),
z2 as (
select 
     distinct on (country_id)
     country_id,
     name 
from y 
order by country_id, sum desc 
),
z3 as (
select 
distinct on (country_id)
     country_id,
     name 
from y 
order by country_id, max desc)
select
zl.country,
zl.name,
z2.name,
z3.name
from zl
join z2 on z2.country_id=zl.country_id
join z3 on zl.country_id=z3.country_id
order by country



