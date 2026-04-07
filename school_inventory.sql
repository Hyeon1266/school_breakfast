CREATE DATABASE IF NOT EXISTS school_inventory
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE school_inventory;

DROP TABLE IF EXISTS inventory_log;
DROP TABLE IF EXISTS menu_recipe;
DROP TABLE IF EXISTS meal_records;
DROP TABLE IF EXISTS contracts;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS suppliers;
DROP TABLE IF EXISTS ingredient_categories;
DROP TABLE IF EXISTS ingredients;
DROP TABLE IF EXISTS schools;

CREATE TABLE schools (
    school_id INT AUTO_INCREMENT PRIMARY KEY,
    school_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    student_cnt INT NOT NULL DEFAULT 0,
    school_type ENUM('초등학교','중학교','고등학교') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_school_name (school_name),
    INDEX idx_region (region),
    INDEX idx_school_type (school_type),
    CHECK (student_cnt >= 0)
) ENGINE=InnoDB;

CREATE TABLE ingredients (
    ingredient_id INT AUTO_INCREMENT PRIMARY KEY,
    ingredient_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    shelf_life INT NOT NULL,
    min_stock DECIMAL(10,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ingredient_name (ingredient_name),
    INDEX idx_category (category),
    CHECK (shelf_life > 0),
    CHECK (min_stock >= 0)
) ENGINE=InnoDB;

-- cost_per_person: 급식 인원 x cost_per_person = 카테고리 추정 수요 비용
CREATE TABLE ingredient_categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    cost_per_person DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_category_name (category_name),
    CHECK (cost_per_person > 0)
) ENGINE=InnoDB;

CREATE TABLE suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    contact VARCHAR(50),
    region VARCHAR(50),
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_supplier_name (supplier_name),
    INDEX idx_supplier_region (region),
    CHECK (is_active IN (0,1))
) ENGINE=InnoDB;

CREATE TABLE deliveries (
    delivery_id INT AUTO_INCREMENT PRIMARY KEY,
    school_id INT NOT NULL,
    ingredient_id INT NOT NULL,
    supplier_id INT NOT NULL,
    delivery_date DATE NOT NULL,
    quantity DECIMAL(10,2) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_del_school FOREIGN KEY (school_id) REFERENCES schools(school_id),
    CONSTRAINT fk_del_ingredient FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id),
    CONSTRAINT fk_del_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
    INDEX idx_delivery_date (delivery_date),
    INDEX idx_school_date (school_id, delivery_date),
    INDEX idx_ingredient_date (ingredient_id, delivery_date),
    CHECK (quantity > 0),
    CHECK (unit_price > 0)
) ENGINE=InnoDB;

-- SUM(contract_amount) vs SUM(actual_meal_cnt x cost_per_person) 으로 공급-수요 불균형 탐지
CREATE TABLE contracts (
    contract_id INT AUTO_INCREMENT PRIMARY KEY,
    school_id INT NOT NULL,
    supplier_id INT NOT NULL,
    category_id INT NOT NULL,
    contract_date DATE NOT NULL,
    delivery_start DATE NOT NULL,
    delivery_end DATE NOT NULL,
    contract_amount DECIMAL(15,2) NOT NULL,
    raw_contract_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_con_school FOREIGN KEY (school_id) REFERENCES schools(school_id),
    CONSTRAINT fk_con_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
    CONSTRAINT fk_con_category FOREIGN KEY (category_id) REFERENCES ingredient_categories(category_id),
    UNIQUE KEY uq_contract (school_id, supplier_id, category_id, contract_date, delivery_start, delivery_end, contract_amount),
    INDEX idx_con_date (contract_date),
    INDEX idx_con_school (school_id, contract_date),
    INDEX idx_con_category (category_id, contract_date),
    CHECK (contract_amount > 0)
) ENGINE=InnoDB;

CREATE TABLE meal_records (
    meal_record_id INT AUTO_INCREMENT PRIMARY KEY,
    school_id INT NOT NULL,
    meal_date DATE NOT NULL,
    actual_meal_cnt INT NOT NULL,
    menu_type VARCHAR(100) NOT NULL,
    waste_rate DECIMAL(5,2),
    note VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_meal_school FOREIGN KEY (school_id) REFERENCES schools(school_id),
    UNIQUE KEY uq_school_date_menu (school_id, meal_date, menu_type),
    INDEX idx_meal_date (meal_date),
    INDEX idx_menu_type (menu_type),
    CHECK (actual_meal_cnt >= 0),
    CHECK (waste_rate IS NULL OR (waste_rate >= 0 AND waste_rate <= 100))
) ENGINE=InnoDB;

CREATE TABLE menu_recipe (
    recipe_id INT AUTO_INCREMENT PRIMARY KEY,
    menu_type VARCHAR(100) NOT NULL,
    ingredient_id INT NOT NULL,
    qty_per_meal DECIMAL(8,4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_recipe_ingredient FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id),
    UNIQUE KEY uq_menu_ingredient (menu_type, ingredient_id),
    INDEX idx_recipe_menu (menu_type),
    CHECK (qty_per_meal > 0)
) ENGINE=InnoDB;

CREATE TABLE inventory_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    school_id INT NOT NULL,
    category_id INT NOT NULL,
    log_date DATE NOT NULL,
    estimated_stock DECIMAL(15,2) NOT NULL,
    predicted_demand DECIMAL(15,2),
    stockout_risk_level TINYINT NOT NULL DEFAULT 0,
    note VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_log_school FOREIGN KEY (school_id) REFERENCES schools(school_id),
    CONSTRAINT fk_log_category FOREIGN KEY (category_id) REFERENCES ingredient_categories(category_id),
    UNIQUE KEY uq_log_school_category_date (school_id, category_id, log_date),
    INDEX idx_log_date (log_date),
    INDEX idx_school_category (school_id, category_id),
    INDEX idx_risk_level (stockout_risk_level),
    CHECK (estimated_stock >= 0),
    CHECK (predicted_demand IS NULL OR predicted_demand >= 0),
    CHECK (stockout_risk_level IN (0,1,2))
) ENGINE=InnoDB;

INSERT INTO ingredient_categories (category_name, cost_per_person) VALUES
('곡류', 320.00),
('육류', 500.00),
('채소류', 120.00),
('유제품', 280.00),
('수산물', 360.00),
('난류', 105.00),
('김치', 80.00),
('가공식품', 350.00),
('두류', 110.00),
('과일류', 200.00);

INSERT INTO schools (school_name, region, student_cnt, school_type) VALUES
('서울초등학교','서울특별시',520,'초등학교'),
('부산중학교','부산광역시',380,'중학교'),
('대구고등학교','대구광역시',610,'고등학교'),
('인천초등학교','인천광역시',470,'초등학교'),
('광주중학교','광주광역시',290,'중학교'),
('대전고등학교','대전광역시',540,'고등학교'),
('수원초등학교','경기도',680,'초등학교'),
('청주중학교','충청북도',310,'중학교');

INSERT INTO ingredients (ingredient_name, category, unit, shelf_life, min_stock) VALUES
('쌀','곡류','kg',365,50.00),
('돼지고기','육류','kg',5,10.00),
('닭고기','육류','kg',5,10.00),
('두부','두류','kg',7,5.00),
('당근','채소류','kg',14,8.00),
('양파','채소류','kg',30,8.00),
('우유','유제품','L',7,20.00),
('계란','난류','개',21,100.00),
('고등어','수산물','kg',3,5.00),
('감자','채소류','kg',20,8.00);

INSERT INTO suppliers (supplier_name, contact, region, is_active) VALUES
('한국식자재(주)','02-1234-5678','서울특별시',1),
('신선푸드','051-234-5678','부산광역시',1),
('농협유통','053-345-6789','대구광역시',1),
('경기친환경농산물','031-456-7890','경기도',1),
('대전수산물유통','042-567-8901','대전광역시',1);

INSERT INTO deliveries (school_id, ingredient_id, supplier_id, delivery_date, quantity, unit_price) VALUES
(1,1,1,'2024-01-03',80.00,2100),(1,2,1,'2024-01-03',15.00,8500),(1,7,1,'2024-01-03',30.00,900),
(2,1,2,'2024-01-04',60.00,2100),(2,3,2,'2024-01-04',12.00,6200),(3,1,3,'2024-01-05',90.00,2100),
(3,8,3,'2024-01-05',150.00,210),(4,5,4,'2024-01-08',20.00,1800),(4,6,4,'2024-01-08',18.00,900),
(5,9,5,'2024-01-08',8.00,4500),(1,1,1,'2024-02-01',75.00,2100),(1,4,1,'2024-02-01',10.00,2200),
(2,1,2,'2024-02-02',58.00,2100),(2,2,2,'2024-02-02',14.00,8500),(3,3,3,'2024-02-05',11.00,6200),
(3,7,3,'2024-02-05',28.00,900),(4,1,4,'2024-02-05',70.00,2100),(5,5,5,'2024-02-06',18.00,1800),
(6,8,1,'2024-02-06',200.00,210),(7,1,4,'2024-02-07',100.00,2100),(1,1,1,'2024-03-04',82.00,2150),
(1,2,1,'2024-03-04',16.00,8600),(2,1,2,'2024-03-05',63.00,2150),(3,9,3,'2024-03-05',7.00,4500),
(4,6,4,'2024-03-06',20.00,910),(5,3,5,'2024-03-06',13.00,6300),(6,7,1,'2024-03-07',35.00,920),
(7,8,4,'2024-03-07',220.00,215),(8,1,2,'2024-03-08',55.00,2150),(8,4,2,'2024-03-08',9.00,2250);

INSERT INTO meal_records (school_id, meal_date, actual_meal_cnt, menu_type, waste_rate, note) VALUES
(1,'2024-01-03',498,'중식',4.2,NULL),(1,'2024-01-04',501,'중식',3.8,NULL),
(1,'2024-01-05',495,'중식',5.1,NULL),(2,'2024-01-04',362,'중식',6.0,NULL),
(2,'2024-01-05',370,'중식',4.5,NULL),(3,'2024-01-05',588,'중식',3.2,NULL),
(3,'2024-01-08',601,'중식',3.9,NULL),(4,'2024-01-08',450,'석식',5.5,NULL),
(1,'2024-02-01',503,'중식',4.0,NULL),(1,'2024-02-02',497,'중식',3.5,NULL),
(2,'2024-02-02',355,'중식',5.8,NULL),(3,'2024-02-05',590,'중식',3.3,NULL),
(4,'2024-02-05',460,'중식',4.7,NULL),(5,'2024-02-06',278,'석식',6.1,NULL),
(6,'2024-02-06',520,'중식',4.4,NULL),(7,'2024-02-07',655,'중식',3.1,'학교 행사'),
(1,'2024-03-04',510,'중식',3.9,NULL),(2,'2024-03-05',368,'중식',5.2,NULL),
(3,'2024-03-05',595,'중식',3.6,NULL),(4,'2024-03-06',455,'석식',4.9,NULL);

INSERT INTO menu_recipe (menu_type, ingredient_id, qty_per_meal) VALUES
-- 중식 (쌀, 돼지고기, 두부, 당근, 양파, 우유)
('중식',1,0.1500),('중식',2,0.0600),('중식',4,0.0400),('중식',5,0.0300),('중식',6,0.0200),('중식',7,0.2000),
-- 석식 (쌀, 닭고기, 계란, 감자, 당근)
('석식',1,0.1500),('석식',3,0.0700),('석식',8,0.5000),('석식',10,0.0400),('석식',5,0.0250);

INSERT INTO inventory_log (school_id, category_id, log_date, estimated_stock, predicted_demand, stockout_risk_level, note) VALUES
(1,2,'2024-03-10',0.00,1500000.00,2,'공급 위험 - 계약금액이 수요 대비 80% 미만'),
(2,3,'2024-03-10',80000.00,95000.00,1,'공급 주의 - 계약금액이 수요 대비 80~120%'),
(3,5,'2024-03-10',0.00,2100000.00,2,'공급 위험 - 계약금액이 수요 대비 80% 미만'),
(4,4,'2024-03-10',500000.00,300000.00,0,'정상'),
(5,6,'2024-03-10',90000.00,105000.00,1,'공급 주의 - 계약금액이 수요 대비 80~120%'),
(6,7,'2024-03-10',320000.00,200000.00,0,'정상'),
(7,1,'2024-03-10',900000.00,1000000.00,1,'공급 주의 - 계약금액이 수요 대비 80~120%'),
(8,4,'2024-03-10',270000.00,300000.00,1,'공급 주의 - 계약금액이 수요 대비 80~120%');

-- 1) 학교/날짜별 식자재 예상 수요
SELECT
    s.school_name, mr.meal_date, mr.menu_type, mr.actual_meal_cnt,
    i.ingredient_name, i.category,
    ROUND(mr.actual_meal_cnt * r.qty_per_meal, 2) AS estimated_demand,
    i.unit
FROM meal_records mr
JOIN schools s ON mr.school_id = s.school_id
JOIN menu_recipe r ON mr.menu_type = r.menu_type
JOIN ingredients i ON r.ingredient_id = i.ingredient_id
ORDER BY mr.meal_date, s.school_name, i.category;

-- 2) 카테고리별 월간 공급 비용 vs 수요 비용 비교
WITH supply AS (
    SELECT school_id, category_id, DATE_FORMAT(delivery_start, '%Y-%m') AS ym,
           SUM(contract_amount) AS supply_cost
    FROM contracts
    GROUP BY school_id, category_id, DATE_FORMAT(delivery_start, '%Y-%m')
),
demand AS (
    SELECT mr.school_id, ic.category_id, DATE_FORMAT(mr.meal_date, '%Y-%m') AS ym,
           SUM(mr.actual_meal_cnt) * ic.cost_per_person AS demand_cost
    FROM meal_records mr
    CROSS JOIN ingredient_categories ic
    GROUP BY mr.school_id, ic.category_id, DATE_FORMAT(mr.meal_date, '%Y-%m'), ic.cost_per_person
)
SELECT
    s.school_name, ic.category_name, sup.ym AS month,
    ROUND(sup.supply_cost, 0) AS supply_cost,
    ROUND(COALESCE(dem.demand_cost, 0), 0) AS demand_cost,
    ROUND(sup.supply_cost / NULLIF(dem.demand_cost, 0) * 100, 1) AS ratio_pct
FROM supply sup
JOIN schools s ON sup.school_id = s.school_id
JOIN ingredient_categories ic ON sup.category_id = ic.category_id
LEFT JOIN demand dem
    ON sup.school_id = dem.school_id
   AND sup.category_id = dem.category_id
   AND sup.ym = dem.ym
ORDER BY month, ratio_pct ASC;

-- 3) 현재 위험 공급-수요 현황
SELECT
    s.school_name, s.region, ic.category_name,
    il.estimated_stock, il.predicted_demand,
    CASE il.stockout_risk_level WHEN 2 THEN '위험' WHEN 1 THEN '주의' ELSE '정상' END AS risk_status,
    il.note, il.log_date
FROM inventory_log il
JOIN schools s ON il.school_id = s.school_id
JOIN ingredient_categories ic ON il.category_id = ic.category_id
WHERE il.stockout_risk_level >= 1
ORDER BY il.stockout_risk_level DESC, il.log_date DESC;

-- 4) 학교별 월간 납품 총액
SELECT
    s.school_name, s.region, DATE_FORMAT(d.delivery_date, '%Y-%m') AS month,
    SUM(d.total_price) AS total_amount, COUNT(*) AS delivery_count
FROM deliveries d
JOIN schools s ON d.school_id = s.school_id
GROUP BY s.school_id, month
ORDER BY month, total_amount DESC;

-- 5) 공급업체별 납품 실적
SELECT
    sp.supplier_name, sp.region,
    COUNT(DISTINCT d.school_id) AS school_count,
    COUNT(*) AS delivery_count,
    SUM(d.total_price) AS total_revenue
FROM deliveries d
JOIN suppliers sp ON d.supplier_id = sp.supplier_id
GROUP BY sp.supplier_id
ORDER BY total_revenue DESC;

-- 6) 최신 위험 등급 요약
SELECT
    CASE stockout_risk_level WHEN 2 THEN '위험' WHEN 1 THEN '주의' ELSE '정상' END AS risk_status,
    COUNT(*) AS count
FROM inventory_log
WHERE log_date = (SELECT MAX(log_date) FROM inventory_log)
GROUP BY stockout_risk_level
ORDER BY stockout_risk_level DESC;
