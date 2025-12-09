SELECT c.customer_id, c.name, SUM(oi.quantity * oi.unit_price) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE p.price > 15
GROUP BY c.customer_id
HAVING total_spent > 200
ORDER BY total_spent DESC
LIMIT 10;


SELECT c.customer_id, c.name,
       (SELECT COUNT(*)
        FROM orders o
        WHERE o.customer_id = c.customer_id) AS order_count
FROM customers c
WHERE c.city = 'City_1'
ORDER BY order_count DESC;


SELECT o.order_id, c.name, o.order_date, o.total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.total > 100 AND c.city = 'City_3'
ORDER BY o.total DESC;


SELECT p.product_id, p.name, COUNT(oi.order_item_id) AS times_ordered
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
WHERE p.price BETWEEN 10 AND 25
GROUP BY p.product_id
ORDER BY times_ordered DESC
LIMIT 15;


SELECT c.customer_id, c.name, o.order_id, o.total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.total > (
      SELECT AVG(total)
      FROM orders
)
ORDER BY o.total DESC;
