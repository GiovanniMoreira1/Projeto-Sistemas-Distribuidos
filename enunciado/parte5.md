# Parte 5: Consistência e replicação

Esta é a última parte do projeto e depende de todo o resto ter sido implementado (e testado) para funcionar. Nesta parte trataremos da réplica dos dados que são recebidos e armazenados pelos servidores.

O broker como implementado para o projeto faz o balanceamento de carga entre os servidores usando o método de round-robin e com isso cada servidor tem uma parte das mensagens trocadas entre os clientes.

Neste caso, se um servidor parar de funcionar temos a perda de uma parte do histórico de mensagens trocadas e se um cliente pedir o histórico de mesagens a um servidor, receberá apenas uma parte do histórico (o que está armazenado naquele servidor). Portanto, é necessário fazer com que mais do que um servidor tenha a cópia dos dados. No caso deste projeto, todos os servidores devem possuir todos os dados.

## Implementação

O método de implementação, assim como o formato das mensagens, é livre. Porém é necessário documentar a escolha.

Escolha uma forma de resolver o problema da replicação dos dados nos servidores dentre as que foram vista na aula de teoria e adicione no `README.md` uma seção explicando como este método resolve o problema do projeto, como foi implementado e se foi necessário realizar alguma mudança no método para funcionar no projeto.

Além disso, altere o código dos servidores para que todos possuam todas as mensagens que foram trocadas.

## Entrega

Nesta última parte será feita a entrega da versão final do projeto contendo:
- Todos os arquivos usados para a execução do projeto
- O código fonte dos servidores com a parte de réplica implementada
- `README.md` com a explicação sobre o método escolhido
