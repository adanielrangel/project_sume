select 
d.id, d.nome,
"siglaPartido","siglaUf",dt_inicio_legslatuara,
dd.sexo,dd."dataNascimento",dd."ufNascimento",
dd."municipioNascimento", dd.escolaridade
from silver.deputados as d
left join deputados.deputados_detalhes as dd 
on d.id = dd.id 
