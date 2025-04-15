function datasufixo() {
    let data = new Date();
    let dia = String(data.getDate() - 1).padStart(2, '0');
    let mes = String(data.getMonth() + 1).padStart(2, '0');
    let ano = data.getFullYear();
    let dataAtual = ano + mes + dia;
    return dataAtual;
}

const GA4_VENDAS = [
    "analytics_153317366"
]


const DATASUFIXO = datasufixo()

module.exports = {
    GA4_VENDAS,
    DATASUFIXO
};
