/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez

	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX State = iota
	wantMX
	inMX
)

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	addresses []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind
}

// ------------------------------------------------------------------------------------
// ------- inicializacao
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		addresses: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		Pp2plink: p2p}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- nucleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {
	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da aplicação
				if dmxR == ENTER {
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO
				} else if dmxR == EXIT {
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				}

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				if strings.Contains(msgOutro.Message, "respOk") {
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO
				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					go module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO em uma goroutine
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicacao
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() {
	module.lcl++
	myTs := module.lcl
	resps := 0

	for _, p := range module.addresses {
		pID, _ := strconv.Atoi(p)
		if pID != module.id {
			go func(peerID int, myTs int) {
				message := fmt.Sprintf("[pl, Send | %d, [reqEntry, %d, %d]]", module.id, module.id, myTs)
				module.sendToLink(p, message, "")
			}(pID, myTs)
		}
	}

	module.st = wantMX
	module.reqTs = myTs

	fmt.Printf("[DEBUG] Process %d: Sent requests for timestamp %d\n", module.id, myTs)

	for resps < module.nbrResps {
		msgOutro := <-module.Pp2plink.Ind
		if strings.Contains(msgOutro.Message, "respOk") {
			module.outDbg("         <<<---- responde! " + msgOutro.Message)
			module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO
			resps++
		}
	}

	fmt.Printf("[DEBUG] Process %d: Received all %d responses\n", module.id, module.nbrResps)

	// Adicionamos um pequeno atraso para simular o tempo de escrita no arquivo
	time.Sleep(500 * time.Millisecond)

	// Use o canal para indicar que terminou a região crítica
	module.Ind <- dmxResp{}
	module.st = inMX
}

func (module *DIMEX_Module) handleUponReqExit() {
	for q, waiting := range module.waiting {
		if waiting {
			module.sendToLink(fmt.Sprint(q), "[pl, Send | "+fmt.Sprint(module.id)+", [respOk, "+fmt.Sprint(module.id)+"]]", "")
		}
	}

	module.st = noMX
	module.waiting = make([]bool, len(module.addresses))
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	module.nbrResps++
	if module.nbrResps == len(module.addresses)-1 {
		module.Ind <- dmxResp{} // Indica que o acesso está livre
		module.st = inMX
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	parts := strings.Fields(msgOutro.Message)
	reqTimestamp, err := strconv.Atoi(strings.TrimRight(parts[len(parts)-1], "]"))

	if err != nil {
		fmt.Println("Erro ao converter timestamp:", err)
		return
	}

	if module.st == noMX || (module.st == wantMX && module.after(module.reqTs, reqTimestamp)) {
		module.sendToLink(fmt.Sprint(msgOutro.From), "[pl, Send | "+fmt.Sprint(module.id)+", [respOk, "+fmt.Sprint(module.id)+"]]", "")
	} else {
		senderID, _ := strconv.Atoi(msgOutro.From)
		module.waiting[senderID] = true
	}

	module.lcl = module.max(module.lcl, reqTimestamp)
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func (module *DIMEX_Module) after(oneTs, othTs int) bool {
	return oneTs > othTs
}

func (module *DIMEX_Module) max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}
