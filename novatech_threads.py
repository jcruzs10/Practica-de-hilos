import threading
import queue
import time
import random
from datetime import datetime

inventory = {
    "P001": {"name": "Teclado mecánico", "stock": 12},
    "P002": {"name": "Mouse inalámbrico", "stock": 18},
    "P003": {"name": "Audífonos USB", "stock": 10},
    "P004": {"name": "Cámara web", "stock": 8},
    "P005": {"name": "Monitor de 24 pulgadas", "stock": 6}
}

order_queue = queue.Queue()

stats = {
    "processed": 0,
    "approved": 0,
    "rejected": 0,
    "failed": 0
}

inv_lock = threading.Lock()    
stats_lock = threading.Lock()  
print_lock = threading.Lock()  
stop_monitor_event = threading.Event() 

active_workers = 0
active_workers_lock = threading.Lock()

def log_message(worker_name, message):
    """Imprime un mensaje con formato de tiempo y nombre del hilo."""
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    with print_lock:
        print(f"[{timestamp}] [{worker_name}] {message}")

def worker(worker_id):
    global active_workers
    worker_name = f"WORKER-{worker_id}"
    
    with active_workers_lock:
        active_workers += 1

    while not order_queue.empty():
        try:
      
            order = order_queue.get_nowait()
        except queue.Empty:
            break
        
        log_message(worker_name, f"Inicia pedido {order.get('id', 'DESCONOCIDO')} | Cliente: {order.get('client', 'Desc')}")
        
        time.sleep(random.uniform(0.5, 2.0))
        
        if "id" not in order or "code" not in order or "qty" not in order or order["qty"] <= 0:
            log_message(worker_name, f"ERROR | Pedido mal formado o inválido: {order}")
            with stats_lock:
                stats["failed"] += 1
                stats["processed"] += 1
            order_queue.task_done()
            continue

        order_id = order["id"]
        prod_code = order["code"]
        qty = order["qty"]

        with inv_lock:
         
            if prod_code not in inventory:
                status = "FALLIDO"
                reason = "Producto no existe"
                with stats_lock: stats["failed"] += 1
            elif inventory[prod_code]["stock"] >= qty:
         
                inventory[prod_code]["stock"] -= qty
                status = "APROBADO"
                reason = f"{prod_code}: -{qty} unidades"
                with stats_lock: stats["approved"] += 1
            else:
   
                status = "RECHAZADO"
                reason = f"Stock insuficiente {prod_code}"
                with stats_lock: stats["rejected"] += 1
   
        
        with stats_lock:
            stats["processed"] += 1

        log_message(worker_name, f"{order_id} {status} | {reason}")
        order_queue.task_done()
        
    with active_workers_lock:
        active_workers -= 1
    log_message(worker_name, "Finalizado. No hay más pedidos.")

def monitor():
    while not stop_monitor_event.is_set():
        with stats_lock:
            approved = stats["approved"]
            rejected = stats["rejected"]
        with active_workers_lock:
            actives = active_workers
        pendings = order_queue.qsize()
        
        log_message("MONITOR", f"Pendientes: {pendings} | Aprobados: {approved} | Rechazados: {rejected} | Activos: {actives}")

        stop_monitor_event.wait(1.5)
    
    log_message("MONITOR", "Señal de detención recibida. Finalizando monitor.")

def cargar_pedidos():
    pedidos = [
        {"id": "ORD-001", "client": "Ana López", "code": "P001", "qty": 2}, 
        {"id": "ORD-002", "client": "Mario Pérez", "code": "P002", "qty": 1},
        {"id": "ORD-003", "client": "Carlos Ruiz", "code": "P005", "qty": 4}, 
        {"id": "ORD-004", "client": "Lucía Gómez", "code": "P005", "qty": 3}, 
        {"id": "ORD-005", "client": "Juan Díaz", "code": "P001", "qty": 15}, 
        {"id": "ORD-007", "client": "José Paz", "code": "P004", "qty": 5},
        {"id": "ORD-008", "client": "Elena M.", "code": "P002", "qty": 2},
        {"id": "ORD-INVALID", "client": "Error", "qty": -5},                 
        {"id": "ORD-009", "client": "Luis M.", "code": "P001", "qty": 1},
        {"id": "ORD-010", "client": "Rosa J.", "code": "P003", "qty": 3},
        {"id": "ORD-011", "client": "Pedro K.", "code": "P004", "qty": 2},
        {"id": "ORD-012", "client": "Sofia L.", "code": "P002", "qty": 5},
        {"id": "ORD-013", "client": "Sara N.", "code": "P001", "qty": 3},
        {"id": "ORD-014", "client": "Raúl C.", "code": "P002", "qty": 8},
        {"id": "ORD-015", "client": "Diana R.", "code": "P004", "qty": 2}, 
        {"id": "ORD-016", "client": "Jorge F.", "code": "P003", "qty": 2},
        {"id": "ORD-017", "client": "Igor W.", "code": "P003", "qty": 2},
        {"id": "ORD-018", "client": "Nina V.", "code": "P001", "qty": 2},
        {"id": "ORD-019", "client": "Paco Q.", "code": "P002", "qty": 1},
        {"id": "ORD-020", "client": "Lia T.", "code": "P005", "qty": 1},   
    ]
    for p in pedidos:
        order_queue.put(p)
    return len(pedidos)

if __name__ == "__main__":
    start_time = time.time()
    total_cargados = cargar_pedidos()
    
    print("--- INICIANDO PROCESAMIENTO DE PEDIDOS NOVATECH ---")
    
    monitor_thread = threading.Thread(target=monitor, daemon=False)
    monitor_thread.start()
    
    workers = []
    for i in range(1, 4):
        t = threading.Thread(target=worker, args=(i,), daemon=False)
        workers.append(t)
        t.start()
        
    order_queue.join()
    
    for t in workers:
        t.join()
        
    stop_monitor_event.set()
    monitor_thread.join()
    
    end_time = time.time()
    
    print("\n--------------------------------------------------------------------")
    print(f"RESUMEN FINAL | Procesados: {stats['processed']} / Cargados: {total_cargados}")
    print(f"Aprobados: {stats['approved']} | Rechazados: {stats['rejected']} | Fallidos/Error: {stats['failed']}")
    print(f"Tiempo total: {end_time - start_time:.2f} s | Hilos finalizados correctamente: {len(workers) + 1}/{len(workers) + 1}")
    print("\n--- INVENTARIO RESTANTE ---")
    for code, data in inventory.items():
        print(f"  {code} ({data['name']}): {data['stock']} unidades")