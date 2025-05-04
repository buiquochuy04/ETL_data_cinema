GO
USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = N'QL')
BEGIN
    ALTER DATABASE [QL] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [QL];
END;

GO
CREATE DATABASE QL
ON 
(
    NAME = N'QL',
    FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\QL.mdf',
    SIZE = 10MB,
    MAXSIZE = 100MB,
    FILEGROWTH = 5MB
)
LOG ON
(
    NAME = N'QL_log',
    FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\QL_log.ldf',
    SIZE = 5MB,
    MAXSIZE = 25MB,
    FILEGROWTH = 5MB
);

GO
USE QL;

GO
CREATE TABLE Cinema (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(100),
                        address NVARCHAR(255),
                        note NVARCHAR(MAX),
                        status NVARCHAR(50)
);

GO
CREATE TABLE Room (
                      id INT IDENTITY(1,1) PRIMARY KEY,
					  name NVARCHAR(100),
                      type NVARCHAR(50),
                      seatCount INT,
                      status NVARCHAR(50),
                      cinema_id INT,
                      FOREIGN KEY (cinema_id) REFERENCES Cinema(id)
);

GO
CREATE TABLE Seat (
                      id INT IDENTITY(1,1) PRIMARY KEY,
                      position NVARCHAR(20),
                      type NVARCHAR(50),
                      status NVARCHAR(50),
                      price DECIMAL(10,2),
                      room_id INT,
                      FOREIGN KEY (room_id) REFERENCES Room(id)
);

GO
CREATE TABLE Movie (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                       title NVARCHAR(255),
                       genre NVARCHAR(100),
                       duration INT,
                       releaseDate DATE,
                       poster NVARCHAR(MAX),
                       trailer NVARCHAR(MAX),
                       description NVARCHAR(MAX),
                       ageRating NVARCHAR(10),
                       status NVARCHAR(50),
                       director NVARCHAR(100),
                       mainActor NVARCHAR(255),
                       language NVARCHAR(50)
);

GO
CREATE TABLE ShowTime (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                          startTime TIME,
                          endTime TIME,
                          showDate DATE,
                          room_id INT,
                          movie_id INT,
                          FOREIGN KEY (room_id) REFERENCES Room(id),
                          FOREIGN KEY (movie_id) REFERENCES Movie(id)
);


GO
CREATE TABLE Customer (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                          fullName NVARCHAR(100),
                          phoneNumber NVARCHAR(20),
                          username NVARCHAR(50),
                          password NVARCHAR(255),
                          email NVARCHAR(100)
);

GO
CREATE TABLE Employee (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                          fullName NVARCHAR(100),
                          phoneNumber NVARCHAR(20),
                          position NVARCHAR(50),
                          username NVARCHAR(50),
                          password NVARCHAR(255),
                          email NVARCHAR(100)
);
GO
CREATE TABLE Discount (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                          name NVARCHAR(100),
                          type NVARCHAR(50),
                          description NVARCHAR(MAX),
                          quantity INT,
                          discountValue DECIMAL(10,2),
                          startDate DATE,
                          endDate DATE
);

GO
CREATE TABLE Product (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                         name NVARCHAR(100),
                         price DECIMAL(10,2),
                         unit NVARCHAR(20),
						 quantity INT,
                         description NVARCHAR(MAX)
);



GO
CREATE TABLE Invoice (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                         createDate DATETIME,
                         totalDiscount DECIMAL(10,2),
                         totalAmount DECIMAL(10,2),
                         paymentMethod NVARCHAR(50),
                         qrCode NVARCHAR(255),
                         status NVARCHAR(50),
                         note NVARCHAR(MAX),
                         customer_id INT,
                         discount_id INT,
                         employee_id INT,
                         FOREIGN KEY (customer_id) REFERENCES Customer(id),
                         FOREIGN KEY (discount_id) REFERENCES Discount(id),
                         FOREIGN KEY (employee_id) REFERENCES Employee(id)
);

GO
CREATE TABLE Ticket (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        bookingDate DATETIME,
                        price DECIMAL(10,2),
                        qrCode NVARCHAR(255),
                        showtime_id INT,
                        seat_id INT,
                        invoice_id INT,
                        foreign key (invoice_id) REFERENCES Invoice(id),
                        FOREIGN KEY (showtime_id) REFERENCES ShowTime(id),
                        FOREIGN KEY (seat_id) REFERENCES Seat(id)
);

GO
CREATE TABLE ProductUsage (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                              quantity INT,
                              purchasePrice DECIMAL(10,2),
                              product_id INT,
                              invoice_id INT,
                              FOREIGN KEY (invoice_id) REFERENCES Invoice(id),
                              FOREIGN KEY (product_id) REFERENCES Product(id)
);

GO
-- Room phụ thuộc Cinema
ALTER TABLE Room
ADD CONSTRAINT FK_Room_Cinema 
FOREIGN KEY (cinema_id) REFERENCES Cinema(id)
ON DELETE CASCADE;
GO
-- Seat phụ thuộc Room
ALTER TABLE Seat
ADD CONSTRAINT FK_Seat_Room 
FOREIGN KEY (room_id) REFERENCES Room(id)
ON DELETE CASCADE;
GO
-- ShowTime phụ thuộc Room và Movie
ALTER TABLE ShowTime
ADD CONSTRAINT FK_ShowTime_Room 
FOREIGN KEY (room_id) REFERENCES Room(id)
ON DELETE CASCADE,
    CONSTRAINT FK_ShowTime_Movie 
FOREIGN KEY (movie_id) REFERENCES Movie(id)
ON DELETE CASCADE;
GO
-- Ticket phụ thuộc ShowTime, Seat và Invoice
ALTER TABLE Ticket
ADD CONSTRAINT FK_Ticket_ShowTime 
FOREIGN KEY (showtime_id) REFERENCES ShowTime(id)
ON DELETE NO ACTION,
    CONSTRAINT FK_Ticket_Seat 
FOREIGN KEY (seat_id) REFERENCES Seat(id)
ON DELETE NO ACTION,
    CONSTRAINT FK_Ticket_Invoice 
FOREIGN KEY (invoice_id) REFERENCES Invoice(id)
ON DELETE CASCADE;
GO
-- ProductUsage phụ thuộc Invoice và Product
ALTER TABLE ProductUsage
ADD CONSTRAINT FK_ProductUsage_Product 
FOREIGN KEY (product_id) REFERENCES Product(id)
ON DELETE NO ACTION, -- Giữ lại dữ liệu sản phẩm dù sản phẩm bị xóa
    CONSTRAINT FK_ProductUsage_Invoice 
FOREIGN KEY (invoice_id) REFERENCES Invoice(id)
ON DELETE CASCADE;
GO
-- Invoice phụ thuộc Customer, Discount, Employee
ALTER TABLE Invoice
ADD CONSTRAINT FK_Invoice_Customer 
FOREIGN KEY (customer_id) REFERENCES Customer(id)
ON DELETE SET NULL,
    CONSTRAINT FK_Invoice_Discount 
FOREIGN KEY (discount_id) REFERENCES Discount(id)
ON DELETE SET NULL,
    CONSTRAINT FK_Invoice_Employee 
FOREIGN KEY (employee_id) REFERENCES Employee(id)
ON DELETE SET NULL;
GO
--Ràng buộc tài khoản duy nhất
ALTER TABLE Customer
ADD CONSTRAINT UQ_Customer_Username UNIQUE (username);
GO
ALTER TABLE Employee
ADD CONSTRAINT UQ_Employee_Username UNIQUE (username);
GO
--Ràng buộc phòng là duy nhất trong rạp
ALTER TABLE Room
ADD CONSTRAINT UQ_Room_Name_Cinema UNIQUE (cinema_id, name);
GO
--Ràng buộc ghế là duy nhất trong phòng
ALTER TABLE Seat
ADD CONSTRAINT UQ_Seat_Position_Room UNIQUE (room_id, position);
GO
--Ràng buộc mã giảm giá là duy nhất
ALTER TABLE Discount
ADD CONSTRAINT UQ_Discount_Name UNIQUE (name);
GO
--Ràng buộc số lượng không âm
ALTER TABLE Room ADD CONSTRAINT CK_Room_SeatCount CHECK (seatCount >= 0);
GO
ALTER TABLE Discount ADD CONSTRAINT CK_Discount_Quantity CHECK (quantity >= 0);
GO -- Số lượng mã giảm giá
ALTER TABLE Product ADD CONSTRAINT CK_Product_Quantity CHECK (quantity >= 0);
GO -- Số lượng tồn kho
ALTER TABLE ProductUsage ADD CONSTRAINT CK_ProductUsage_Quantity CHECK (quantity > 0);
GO -- Thường phải mua ít nhất 1
ALTER TABLE Movie ADD CONSTRAINT CK_Movie_Duration CHECK (duration > 0); -- Thời lượng phải dương

GO
ALTER TABLE Cinema
ADD CONSTRAINT CK_Cinema_Status 
CHECK (status IN (N'Hoạt động', N'Đang bảo trì', N'Đóng cửa'));

GO
ALTER TABLE Room
ADD CONSTRAINT CK_Room_Status 
CHECK (status IN (N'Hoạt động', N'Đang bảo trì', N'Đóng cửa'));

GO
ALTER TABLE Seat
ADD CONSTRAINT CK_Seat_Status 
CHECK (status IN (N'Trống', N'Đã đặt', N'Bảo trì'));

GO
ALTER TABLE Movie
ADD CONSTRAINT CK_Movie_Status 
CHECK (status IN (N'Đang chiếu', N'Sắp chiếu', N'Ngừng chiếu'));

GO
ALTER TABLE Invoice
ADD CONSTRAINT CK_Invoice_Status 
CHECK (status IN (N'Đã thanh toán', N'Chưa thanh toán', N'Đã hủy'));

GO
-- Tạo kiểu dữ liệu bảng để sử dụng trong tham số của stored procedure
CREATE TYPE InvoiceIdTableType AS TABLE (invoice_id INT PRIMARY KEY);
GO

-- Thủ tục tính toán tổng hóa đơn
-- Nhận một bảng các ID hóa đơn cần cập nhật và tính toán lại tổng tiền, chiết khấu
CREATE OR ALTER PROCEDURE usp_UpdateInvoiceTotal
    @InvoiceIds InvoiceIdTableType READONLY -- Tham số đầu vào là bảng chứa ID hóa đơn cần cập nhật
AS
BEGIN
    SET NOCOUNT ON; -- Tắt thông báo số dòng bị ảnh hưởng để tăng hiệu suất
    
    -- Cập nhật thông tin tổng tiền và chiết khấu cho các hóa đơn cần cập nhật
    UPDATE inv
    SET
        -- Cập nhật tổng chiết khấu, nếu NULL thì gán là 0
        inv.totalDiscount = ISNULL(discCalc.CalculatedDiscount, 0),
        
        -- Cập nhật tổng tiền: tổng(vé + sản phẩm - chiết khấu), đảm bảo không âm
        inv.totalAmount = CASE
                            WHEN (ISNULL(ticketTotals.TotalTicketPrice, 0) + ISNULL(productTotals.TotalProductPrice, 0) - ISNULL(discCalc.CalculatedDiscount, 0)) < 0 THEN 0
                            ELSE (ISNULL(ticketTotals.TotalTicketPrice, 0) + ISNULL(productTotals.TotalProductPrice, 0) - ISNULL(discCalc.CalculatedDiscount, 0))
                         END
    FROM Invoice inv
    -- Kết nối với bảng ID hóa đơn cần cập nhật
    INNER JOIN @InvoiceIds aff ON inv.id = aff.invoice_id
    
    -- Tính tổng tiền vé từ bảng Ticket
    LEFT JOIN (
        SELECT invoice_id, SUM(price) AS TotalTicketPrice
        FROM Ticket
        WHERE invoice_id IN (SELECT invoice_id FROM @InvoiceIds)
        GROUP BY invoice_id
    ) AS ticketTotals ON inv.id = ticketTotals.invoice_id
    
    -- Tính tổng tiền sản phẩm từ bảng ProductUsage
    LEFT JOIN (
        SELECT invoice_id, SUM(quantity * purchasePrice) AS TotalProductPrice
        FROM ProductUsage
        WHERE invoice_id IN (SELECT invoice_id FROM @InvoiceIds)
        GROUP BY invoice_id
    ) AS productTotals ON inv.id = productTotals.invoice_id
    
    -- Lấy thông tin giảm giá từ bảng Discount
    LEFT JOIN Discount d ON inv.discount_id = d.id
    
    -- Sử dụng CROSS APPLY để tính toán chiết khấu dựa trên loại giảm giá
    CROSS APPLY (
        SELECT
            CASE
                WHEN d.id IS NULL THEN 0 -- Không có giảm giá
                WHEN d.type = 'Fixed' THEN d.discountValue -- Giảm giá cố định
                WHEN d.type = 'Percentage' THEN -- Giảm giá theo phần trăm
                    ((ISNULL(ticketTotals.TotalTicketPrice, 0) + ISNULL(productTotals.TotalProductPrice, 0)) * d.discountValue / 100.0)
                ELSE 0 -- Loại giảm giá không xác định
            END AS CalculatedDiscount
    ) AS discCalc;
END
GO

-- Trigger cập nhật tổng hóa đơn khi thêm/sửa/xóa vé
CREATE OR ALTER TRIGGER TR_Ticket_UpdateInvoiceTotal
ON Ticket
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Tạo bảng biến để lưu ID các hóa đơn bị ảnh hưởng
    DECLARE @AffectedInvoices InvoiceIdTableType;
    
    -- Lấy ID hóa đơn từ các vé được thêm vào hoặc cập nhật
    INSERT INTO @AffectedInvoices (invoice_id)
    SELECT DISTINCT invoice_id FROM inserted
    WHERE invoice_id IS NOT NULL;
    
    -- Lấy ID hóa đơn từ các vé bị xóa (tránh trùng lặp)
    INSERT INTO @AffectedInvoices (invoice_id)
    SELECT DISTINCT invoice_id FROM deleted
    WHERE invoice_id IS NOT NULL
      AND invoice_id NOT IN (SELECT invoice_id FROM @AffectedInvoices);
      
    -- Chỉ thực hiện nếu có hóa đơn bị ảnh hưởng
    IF EXISTS (SELECT 1 FROM @AffectedInvoices)
    BEGIN
        -- Gọi thủ tục để cập nhật tổng tiền hóa đơn
        EXEC usp_UpdateInvoiceTotal @AffectedInvoices;
    END
END;
GO

-- Trigger cập nhật tổng hóa đơn khi thêm/sửa/xóa sản phẩm
CREATE OR ALTER TRIGGER TR_ProductUsage_UpdateInvoiceTotal
ON ProductUsage
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Tạo bảng biến để lưu ID các hóa đơn bị ảnh hưởng
    DECLARE @AffectedInvoices InvoiceIdTableType;
    
    -- Lấy ID hóa đơn từ các sản phẩm được thêm vào hoặc cập nhật
    INSERT INTO @AffectedInvoices (invoice_id)
    SELECT DISTINCT invoice_id FROM inserted
    WHERE invoice_id IS NOT NULL;
    
    -- Lấy ID hóa đơn từ các sản phẩm bị xóa (tránh trùng lặp)
    INSERT INTO @AffectedInvoices (invoice_id)
    SELECT DISTINCT invoice_id FROM deleted
    WHERE invoice_id IS NOT NULL
      AND invoice_id NOT IN (SELECT invoice_id FROM @AffectedInvoices);
      
    -- Chỉ thực hiện nếu có hóa đơn bị ảnh hưởng
    IF EXISTS (SELECT 1 FROM @AffectedInvoices)
    BEGIN
        -- Gọi thủ tục để cập nhật tổng tiền hóa đơn
        EXEC usp_UpdateInvoiceTotal @AffectedInvoices;
    END
END;
GO

-- Trigger cập nhật tổng hóa đơn khi thay đổi mã giảm giá
CREATE OR ALTER TRIGGER TR_Invoice_DiscountChange_UpdateTotal
ON Invoice
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Chỉ tiếp tục nếu cột discount_id đã thay đổi
    IF UPDATE(discount_id)
    BEGIN
        -- Tạo bảng biến để lưu ID các hóa đơn bị ảnh hưởng
        DECLARE @AffectedInvoices InvoiceIdTableType;
        
        -- Lấy ID hóa đơn từ các hóa đơn được cập nhật
        INSERT INTO @AffectedInvoices (invoice_id)
        SELECT DISTINCT id FROM inserted;
        
        -- Chỉ thực hiện nếu có hóa đơn bị ảnh hưởng
        IF EXISTS (SELECT 1 FROM @AffectedInvoices)
        BEGIN
            -- Gọi thủ tục để cập nhật tổng tiền hóa đơn
            EXEC usp_UpdateInvoiceTotal @AffectedInvoices;
        END
    END;
END;
GO

-- Trigger kiểm tra và xử lý tồn kho khi thêm sản phẩm vào hóa đơn
CREATE OR ALTER TRIGGER trg_CheckProductStock
ON ProductUsage
INSTEAD OF INSERT -- Sử dụng INSTEAD OF để có thể kiểm tra trước khi thực sự chèn dữ liệu
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Tìm tất cả sản phẩm không đủ số lượng trong kho trong một truy vấn
    DECLARE @InsufficientProducts TABLE (
        product_id INT,
        product_name NVARCHAR(100),
        requested INT,
        available INT
    );
    
    -- Chèn thông tin các sản phẩm không đủ số lượng
    INSERT INTO @InsufficientProducts
    SELECT i.product_id, p.name, i.quantity, p.quantity
    FROM inserted i
    JOIN Product p ON i.product_id = p.id
    WHERE i.quantity > p.quantity;
    
    -- Nếu có bất kỳ sản phẩm nào không đủ số lượng, trả về thông báo lỗi chi tiết
    IF EXISTS (SELECT 1 FROM @InsufficientProducts)
    BEGIN
        DECLARE @ErrorMessage NVARCHAR(MAX) = N'Không đủ số lượng trong kho cho các sản phẩm sau:';
        
        -- Tạo thông báo lỗi chi tiết cho từng sản phẩm
        SELECT @ErrorMessage = @ErrorMessage + CHAR(13) + N' - ' + product_name + 
                             N': Yêu cầu ' + CAST(requested AS NVARCHAR) + 
                             N', Có sẵn ' + CAST(available AS NVARCHAR)
        FROM @InsufficientProducts;
        
        RAISERROR (@ErrorMessage, 16, 1); -- Phát sinh lỗi với mức độ 16 (lỗi người dùng)
        RETURN;
    END
    
    -- Cập nhật số lượng sản phẩm trong kho trong một lần cập nhật duy nhất
    UPDATE p
    SET p.quantity = p.quantity - i.quantity
    FROM Product p
    JOIN inserted i ON p.id = i.product_id;
    
    -- Chèn vào ProductUsage, tự động lấy giá nếu purchasePrice là NULL
    INSERT INTO ProductUsage (quantity, purchasePrice, product_id, invoice_id)
    SELECT
        i.quantity,
        ISNULL(i.purchasePrice, p.price), -- Nếu NULL thì lấy giá hiện tại của sản phẩm
        i.product_id,
        i.invoice_id
    FROM inserted i
    JOIN Product p ON i.product_id = p.id;
END
GO

-- Trigger khôi phục số lượng sản phẩm khi xóa khỏi hóa đơn
CREATE OR ALTER TRIGGER trg_RestoreProductStock_AfterProductUsageDelete
ON ProductUsage
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Cập nhật lại số lượng sản phẩm trong kho khi xóa khỏi hóa đơn
    UPDATE p
    SET p.quantity = p.quantity + d.quantity -- Cộng lại số lượng đã trừ trước đó
    FROM Product p
    JOIN deleted d ON p.id = d.product_id;
END
GO

-- Trigger xử lý đặt vé và trạng thái ghế
CREATE OR ALTER TRIGGER trg_ManageTicketAndSeat
ON Ticket
INSTEAD OF INSERT -- Sử dụng INSTEAD OF để kiểm tra trước khi chèn
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Kiểm tra các ghế đã được đặt
    DECLARE @BookedSeats TABLE (
        seat_id INT,
        seat_position NVARCHAR(20)
    );
    
    -- Chèn thông tin các ghế đã được đặt
    INSERT INTO @BookedSeats
    SELECT i.seat_id, s.position
    FROM inserted i
    JOIN Seat s ON i.seat_id = s.id
    WHERE s.status = N'Đã đặt';
    
    -- Nếu có bất kỳ ghế nào đã được đặt, trả về thông báo lỗi chi tiết
    IF EXISTS (SELECT 1 FROM @BookedSeats)
    BEGIN
        DECLARE @ErrorMessage NVARCHAR(MAX) = N'Không thể đặt các ghế sau vì đã được đặt:';
        
        -- Tạo thông báo lỗi chi tiết cho từng ghế
        SELECT @ErrorMessage = @ErrorMessage + CHAR(13) + N' - Ghế ' + seat_position
        FROM @BookedSeats;
        
        RAISERROR (@ErrorMessage, 16, 1); -- Phát sinh lỗi với mức độ 16 (lỗi người dùng)
        RETURN;
    END
    
    -- Đặt giá vé từ giá ghế nếu không được chỉ định
    DECLARE @TicketsWithPrices TABLE (
        bookingDate DATETIME,
        price DECIMAL(10,2),
        showtime_id INT,
        seat_id INT,
        invoice_id INT
    );
    
    -- Chèn thông tin vé với giá chính xác
    INSERT INTO @TicketsWithPrices
    SELECT 
        i.bookingDate,
        ISNULL(i.price, s.price), -- Sử dụng giá ghế nếu giá vé là NULL
        i.showtime_id,
        i.seat_id,
        i.invoice_id
    FROM inserted i
    JOIN Seat s ON i.seat_id = s.id;
    
    -- Chèn vé với giá đã được xử lý
    INSERT INTO Ticket (bookingDate, price, showtime_id, seat_id, invoice_id)
    SELECT bookingDate, price, showtime_id, seat_id, invoice_id
    FROM @TicketsWithPrices;
    
    -- Cập nhật trạng thái ghế thành 'Đã đặt'
    UPDATE s
    SET s.status = N'Đã đặt'
    FROM Seat s
    JOIN inserted i ON s.id = i.seat_id;
END
GO

-- Trigger đặt lại trạng thái ghế khi xóa vé
CREATE OR ALTER TRIGGER trg_ResetSeatStatus_AfterTicketDelete
ON Ticket
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Đặt lại trạng thái ghế thành 'Trống' trong một lệnh
    UPDATE s
    SET s.status = N'Trống'
    FROM Seat s
    JOIN deleted d ON s.id = d.seat_id;
END
GO

-- Trigger quản lý xuất chiếu
CREATE OR ALTER TRIGGER trg_ManageShowTime
ON ShowTime
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Cập nhật giờ kết thúc dựa trên thời lượng phim
    IF UPDATE(startTime) OR UPDATE(movie_id) OR EXISTS (SELECT 1 FROM inserted i WHERE i.endTime IS NULL)
    BEGIN
        UPDATE st
        SET st.endTime = DATEADD(MINUTE, m.duration, st.startTime)
        FROM ShowTime st
        JOIN inserted i ON st.id = i.id
        JOIN Movie m ON st.movie_id = m.id;
    END
    
    -- Kiểm tra lịch chiếu trùng lặp
    DECLARE @OverlappingShows TABLE (
        show_id INT,
        room_name NVARCHAR(100),
        showDate DATE,
        conflictTime NVARCHAR(100)
    );
    
    -- Tìm các lịch chiếu trùng lặp
    INSERT INTO @OverlappingShows
    SELECT 
        i.id,
        r.name,
        i.showDate,
        CONCAT(CONVERT(VARCHAR, i.startTime, 108), ' - ', CONVERT(VARCHAR, i.endTime, 108))
    FROM inserted i
    JOIN ShowTime st ON st.room_id = i.room_id
                    AND st.id <> i.id
                    AND st.showDate = i.showDate
    JOIN Room r ON r.id = i.room_id
    WHERE
        -- Lịch mới bắt đầu TRONG lúc lịch cũ đang chiếu
        (i.startTime >= st.startTime AND i.startTime < st.endTime)
        OR
        -- Lịch mới kết thúc TRONG lúc lịch cũ đang chiếu
        (i.endTime > st.startTime AND i.endTime <= st.endTime)
        OR
        -- Lịch mới bao trùm hoàn toàn lịch cũ
        (i.startTime <= st.startTime AND i.endTime >= st.endTime);
    
    -- Nếu có lịch chiếu trùng lặp, trả về thông báo lỗi chi tiết
    IF EXISTS (SELECT 1 FROM @OverlappingShows)
    BEGIN
        DECLARE @ErrorMsg NVARCHAR(MAX) = N'Không thể lưu lịch chiếu do trùng lặp với lịch chiếu khác:';
        
        -- Tạo thông báo lỗi chi tiết cho từng lịch chiếu trùng lặp
        SELECT @ErrorMsg = @ErrorMsg + CHAR(13) + N' - Phòng ' + room_name + 
                         N' ngày ' + CONVERT(NVARCHAR, showDate, 103) + 
                         N' thời gian ' + conflictTime
        FROM @OverlappingShows;
        
        RAISERROR (@ErrorMsg, 16, 1); -- Phát sinh lỗi với mức độ 16 (lỗi người dùng)
        ROLLBACK TRANSACTION;
        RETURN;
    END
END
GO

-- Trigger tạo mã QR cho vé
CREATE OR ALTER TRIGGER trg_GenerateQRCode_Ticket
ON Ticket
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Chỉ cập nhật nếu các cột liên quan thay đổi hoặc có bản ghi mới
    IF EXISTS(SELECT 1 FROM inserted) OR UPDATE(showtime_id) OR UPDATE(seat_id)
    BEGIN
        -- Cập nhật mã QR cho vé với định dạng dễ đọc
        UPDATE t
        SET t.qrCode = CONCAT(
            'TICKET#', t.id,
            '|SHOW#', ISNULL(t.showtime_id, 0),
            '|SEAT:', ISNULL(s.position, 'N/A'),
            '|DATE:', FORMAT(ISNULL(st.showDate, GETDATE()), 'yyyy-MM-dd'),
            '|TIME:', FORMAT(ISNULL(st.startTime, CAST('00:00:00' AS TIME)), 'HH:mm')
        )
        FROM Ticket t
        INNER JOIN inserted i ON t.id = i.id
        LEFT JOIN Seat s ON t.seat_id = s.id
        LEFT JOIN ShowTime st ON t.showtime_id = st.id;
    END
END
GO

-- Trigger tạo mã QR cho hóa đơn
CREATE OR ALTER TRIGGER trg_GenerateQRCode_Invoice
ON Invoice
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Chỉ cập nhật nếu các cột liên quan thay đổi hoặc có bản ghi mới
    IF EXISTS(SELECT 1 FROM inserted) OR UPDATE(createDate) OR UPDATE(totalAmount)
    BEGIN
        -- Cập nhật mã QR cho hóa đơn với định dạng dễ đọc
        UPDATE inv
        SET inv.qrCode = CONCAT(
            'INVOICE#', inv.id,
            '|DATE:', FORMAT(ISNULL(inv.createDate, GETDATE()), 'yyyy-MM-dd HH:mm'),
            '|TOTAL:', FORMAT(ISNULL(inv.totalAmount, 0), 'N0'),
            '|PAYMENT:', ISNULL(inv.paymentMethod, N'N/A')
        )
        FROM Invoice inv
        INNER JOIN inserted i ON inv.id = i.id;
    END
END
GO

-- Trigger xử lý khi xóa hóa đơn
CREATE OR ALTER TRIGGER trg_ResetStockAndSeat_AfterInvoiceDelete
ON Invoice
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Đặt lại trạng thái ghế thành 'Trống' trong một truy vấn
    UPDATE s
    SET s.status = N'Trống'
    FROM Seat s
    JOIN Ticket t ON s.id = t.seat_id
    JOIN deleted d ON t.invoice_id = d.id;
    
    -- Khôi phục số lượng sản phẩm trong kho trong một truy vấn
    UPDATE p
    SET p.quantity = p.quantity + pu.quantity
    FROM Product p
    JOIN ProductUsage pu ON p.id = pu.product_id
    JOIN deleted d ON pu.invoice_id = d.id;
END
GO