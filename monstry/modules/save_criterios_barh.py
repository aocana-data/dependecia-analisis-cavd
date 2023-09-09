def save_criterios_generales(**kwargs):
    
    criterio_min    =  kwargs["criterio_min"]
    title           =  kwargs["title"]
    path            =  kwargs["path"] 

    cm          =   criterio_min.iloc[::-1]
    font_size   =   10
        
    bar_cm = cm.plot.barh(
        y           =   "CRITERIO MINIMO",
        figsize     =   (16,len(cm)/2) ,
        fontsize    =   font_size ,
        color       =   "#077b8a" ,
        title       =   title
    )

    bar_cm.bar_label(bar_cm.containers[0],fontsize = font_size,fmt="%g %%")
    bar_cm.legend(loc='center left',bbox_to_anchor=(1.0, 0.5))

    bar_cm.figure.savefig(path)