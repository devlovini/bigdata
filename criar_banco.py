import sqlite3

def criar_banco():
    conn = sqlite3.connect('estoque.db') 
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY,
            nome TEXT,
            quantidade INTEGER,
            preco REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vendas (
            id_venda INTEGER PRIMARY KEY,
            id_produto INTEGER,
            quantidade INTEGER,
            valor_total REAL
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Banco de dados criado com sucesso!")


criar_banco()

def adicionar_produto(id, nome, quantidade, preco):
    conn = sqlite3.connect('estoque.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO produtos (id, nome, quantidade, preco)
        VALUES (?, ?, ?, ?)
    ''', (id, nome, quantidade, preco))
    
    conn.commit()
    conn.close()
    print(f"Produto {nome} adicionado com sucesso!")

